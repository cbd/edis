%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis Command runner.
%%%      It helps pipelining commands and running them in order, thanks to
%%%      regular Erlang mechanisms
%%% @todo Unsupported commands: SYNC, SLOWLOG, SLAVEOF
%%% @end
%%%-------------------------------------------------------------------
-module(edis_command_runner).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(gen_server).

-include("edis.hrl").

-record(state, {socket                    :: port(),
                db = edis_db:process(0)   :: atom(),
                db_index = 0              :: non_neg_integer(),
                peerport                  :: pos_integer(),
                authenticated = false     :: boolean(),
                multi_queue = undefined   :: undefined | [{binary(), [binary()]}],
                watched_keys = []         :: [{binary(), undefined | non_neg_integer()}],
                subscriptions = undefined :: undefined | {gb_set(), gb_set()}}).
-opaque state() :: #state{}.

-export([start_link/1, stop/1, err/2, run/3]).
-export([last_arg/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% =================================================================================================
%% External functions
%% =================================================================================================
%% @doc starts a new command runner
-spec start_link(port()) -> {ok, pid()}.
start_link(Socket) ->
  gen_server:start_link(?MODULE, Socket, []).

%% @doc stops the connection
-spec stop(pid()) -> ok.
stop(Runner) ->
  gen_server:cast(Runner, stop).

%% @doc generates an error(like throw())
-spec err(pid(), iodata()) -> ok.
err(Runner, Message) ->
  gen_server:cast(Runner, {err, Message}).

%% @doc executes the received command
-spec run(pid(), binary(), [binary()]) -> ok.
run(Runner, Command, Arguments) ->
  gen_server:cast(Runner, {run, Command, Arguments}).

%% @doc should last argument be inlined?
%%      Useful for old protocol calls.
-spec last_arg(binary()) -> inlined | safe.
last_arg(_) -> inlined.

%% =================================================================================================
%% Server functions
%% =================================================================================================
%% @hidden
-spec init(port()) -> {ok, state()}.
init(Socket) ->
  PeerPort =
    try inet:peername(Socket) of
      {ok, {_Ip, Port}} -> Port;
      Error -> Error
    catch
      _:Error -> Error
    end,
  Authenticated = undefined =:= edis_config:get(requirepass),
  {ok, #state{socket = Socket, peerport = PeerPort, authenticated = Authenticated}}.

%% @hidden
-spec handle_call(X, reference(), state()) -> {stop, {unexpected_request, X}, {unexpected_request, X}, state()}.
handle_call(X, _From, State) -> {stop, {unexpected_request, X}, {unexpected_request, X}, State}.

%% @hidden
-spec handle_cast(stop | {err, binary()} | {run, binary(), [binary()]}, state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast({err, Message}, State) ->
  tcp_err(Message, State);
handle_cast({run, Cmd, Args}, State) ->
  try
    OriginalCommand = #edis_command{cmd = Cmd,
                                    db = State#state.db_index,
                                    args = Args},
    Command = parse_command(OriginalCommand),
    ok = edis_db_monitor:notify(OriginalCommand),
    case {State#state.multi_queue, State#state.subscriptions} of
      {undefined, undefined} -> run(Command, State);
      {undefined, _InPubSub} -> pubsub(Command, State);
      {_InMulti, undefined} -> queue(Command, State);
      {_InMulti, _InPubSub} -> throw(invalid_context)
    end
  catch
    _:timeout ->
      tcp_multi_bulk(undefined, State);
    _:invalid_password ->
      lager:warning("Invalid password.~n", []),
      tcp_err(<<"invalid password">>, State#state{authenticated = false});
    _:Error ->
      lager:error("Error in db ~p: ~p~nStack: ~p", [State#state.db_index, Error, erlang:get_stacktrace()]),
      tcp_err(parse_error(Cmd, Error), State)
  end.

%% @hidden
-spec handle_info(term(), state()) -> {noreply, state(), hibernate}.
handle_info(#edis_message{} = Message, State = #state{subscriptions = undefined}) ->
  lager:warning("Unexpected message: ~p~n", [Message]),
  {noreply, State, hibernate};
handle_info(#edis_message{} = Message, State) ->
  {ChannelSet, PatternSet} = State#state.subscriptions,
  case gb_sets:is_member(Message#edis_message.channel, ChannelSet) of
    true ->
      tcp_multi_bulk([<<"message">>, Message#edis_message.channel, Message#edis_message.message], State);
    false ->
      case gb_sets:fold(fun(_, true) -> true;
                           (Pattern, false) ->
                                re:run(Message#edis_message.channel, Pattern) /= nomatch
                        end, false, PatternSet) of
        true ->
          tcp_multi_bulk([<<"pmessage">>, Message#edis_message.channel, Message#edis_message.message], State);
        false ->
          {noreply, State, hibernate}
      end
  end;
handle_info(#edis_command{db = 0} = Command, State) ->
  tcp_string(io_lib:format("~p ~s ~s", [Command#edis_command.timestamp,
                                        Command#edis_command.cmd,
                                        edis_util:join(Command#edis_command.args, <<" ">>)]), State);
handle_info(#edis_command{} = Command, State) ->
  tcp_string(io_lib:format("~p (db ~p) ~s ~s", [Command#edis_command.timestamp,
                                                Command#edis_command.db,
                                                Command#edis_command.cmd,
                                                edis_util:join(Command#edis_command.args, <<" ">>)]), State);
handle_info({gen_event_EXIT, _Handler, Reason}, State) ->
  lager:info("Monitor deactivated. Reason: ~p~n", [Reason]),
  {noreply, State, hibernate};
handle_info(Info, State) ->
  lager:info("Unexpected info: ~p~n", [Info]),
  {noreply, State, hibernate}.

%% @hidden
-spec terminate(term(), state()) -> ok.
terminate(_, _) -> ok.

%% @hidden
-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
-spec parse_command(#edis_command{args :: [binary()]}) -> #edis_command{}.
parse_command(C = #edis_command{cmd = <<"QUIT">>, args = []}) -> C#edis_command{result_type = ok, group=connection};
parse_command(#edis_command{cmd = <<"QUIT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"AUTH">>, args = [_Password]}) -> C#edis_command{result_type = ok, group=connection};
parse_command(#edis_command{cmd = <<"AUTH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SELECT">>, args = [Db]}) ->
  case {edis_util:binary_to_integer(Db, 0), edis_config:get(databases)} of
    {DbIndex, Dbs} when DbIndex < 0 orelse DbIndex >= Dbs -> throw({error, "invalid DB index"});
    {DbIndex, _} -> C#edis_command{args = [DbIndex], result_type = ok, group=connection}
  end;
parse_command(#edis_command{cmd = <<"SELECT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"PING">>, args = []}) -> C#edis_command{result_type = string, group=connection};
parse_command(#edis_command{cmd = <<"PING">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ECHO">>, args = [_Word]}) -> C#edis_command{result_type = bulk, group=connection};
parse_command(#edis_command{cmd = <<"ECHO">>}) -> throw(bad_arg_num);
%% -- Strings --------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"APPEND">>, args = [_Key, _Value]}) -> C#edis_command{result_type = number, group=strings};
parse_command(#edis_command{cmd = <<"APPEND">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"DECR">>, args = [_Key]}) -> C#edis_command{result_type = number, group=strings};
parse_command(#edis_command{cmd = <<"DECR">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"DECRBY">>, args = [Key, Decrement]}) -> 
  C#edis_command{args = [Key, edis_util:binary_to_integer(Decrement)],result_type = number, group=strings};
parse_command(#edis_command{cmd = <<"DECRBY">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"GET">>, args = [_Key]}) -> C#edis_command{result_type = bulk, group=strings};
parse_command(#edis_command{cmd = <<"GET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"GETBIT">>, args = [Key, Offset]}) ->
  try edis_util:binary_to_integer(Offset) of
    O when O >= 0 -> C#edis_command{args = [Key, O], result_type = number, group=strings};
    _ -> throw({not_integer, "bit offset"})
  catch
    _:not_integer -> throw({not_integer, "bit offset"})
  end;
parse_command(#edis_command{cmd = <<"GETBIT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"GETRANGE">>, args = [Key, Start, End]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start), edis_util:binary_to_integer(End)], result_type = bulk, group=strings};
parse_command(#edis_command{cmd = <<"GETRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"GETSET">>, args = [_Key, _Value]}) -> C#edis_command{result_type = bulk, group=strings};
parse_command(#edis_command{cmd = <<"GETSET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"INCR">>, args = [_Key]}) -> C#edis_command{result_type = number, group=strings};
parse_command(#edis_command{cmd = <<"INCR">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"INCRBY">>, args = [Key, Increment]}) -> 
  C#edis_command{args = [Key, edis_util:binary_to_integer(Increment)], result_type = number, group=strings};
parse_command(#edis_command{cmd = <<"INCRBY">>}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"MGET">>, args = []}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"MGET">>}) -> C#edis_command{result_type = multi_bulk, group=strings};
parse_command(C = #edis_command{cmd = <<"MSET">>, args = KVs}) when KVs =/= [], length(KVs) rem 2 =:= 0 -> 
  C#edis_command{args = edis_util:make_pairs(KVs), result_type = ok, group=strings};
parse_command(#edis_command{cmd = <<"MSET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"MSETNX">>, args = KVs}) when KVs =/= [], length(KVs) rem 2 =:= 0 ->
  C#edis_command{args = edis_util:make_pairs(KVs), result_type = number, group=strings};
parse_command(#edis_command{cmd = <<"MSETNX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SET">>, args = [_Key, _Value]}) -> C#edis_command{ result_type = ok, group=strings};
parse_command(#edis_command{cmd = <<"SET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SETBIT">>, args = [Key, Offset, Bit]}) ->
  try {edis_util:binary_to_integer(Offset), Bit} of
    {O, Bit} when O >= 0, Bit == <<"0">> -> C#edis_command{args = [Key, O, 0], result_type = number, group=strings};
    {O, Bit} when O >= 0, Bit == <<"1">> -> C#edis_command{args = [Key, O, 1], result_type = number, group=strings};
    {O, _BadBit} when O >= 0 -> throw({not_integer, "bit"});
    _ -> throw({not_integer, "bit offset"})
  catch
    _:not_integer -> throw({not_integer, "bit offset"})
  end;
parse_command(#edis_command{cmd = <<"SETBIT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SETEX">>, args = [Key, Seconds, Value]}) ->
  case edis_util:binary_to_integer(Seconds) of
    Secs when Secs =< 0 -> throw({error, "invalid expire time in SETEX"});
    Secs -> C#edis_command{args = [Key, Secs, Value], result_type = ok, group=strings}
  end;
parse_command(#edis_command{cmd = <<"SETEX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SETNX">>, args = [_Key, _Value]}) -> C#edis_command{result_type = number, group=strings};
parse_command(#edis_command{cmd = <<"SETNX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SETRANGE">>, args = [Key, Offset, Value]}) ->
  case edis_util:binary_to_integer(Offset) of
    Off when Off < 0 -> throw({out_of_range, "offset"});
    Off -> C#edis_command{args = [Key, Off, Value], result_type = number, group=strings}
  end;
parse_command(#edis_command{cmd = <<"SETRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"STRLEN">>, args = [_Key]}) -> C#edis_command{result_type = number, group=strings};
parse_command(#edis_command{cmd = <<"STRLEN">>}) -> throw(bad_arg_num);
%% -- Keys -----------------------------------------------------------------------------------------
parse_command(#edis_command{cmd = <<"DEL">>, args = []}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"DEL">>}) -> C#edis_command{result_type = number, group=keys};
parse_command(C = #edis_command{cmd = <<"EXISTS">>, args = [_Key]}) -> C#edis_command{result_type = boolean, group=keys};
parse_command(#edis_command{cmd = <<"EXISTS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"EXPIRE">>, args = [Key, Seconds]}) -> 
  C#edis_command{args = [Key, edis_util:binary_to_integer(Seconds)], result_type = boolean, group=keys};
parse_command(#edis_command{cmd = <<"EXPIRE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"EXPIREAT">>, args = [Key, Timestamp]}) -> 
  C#edis_command{args = [Key, edis_util:binary_to_integer(Timestamp)],result_type = boolean, group=keys};
parse_command(#edis_command{cmd = <<"EXPIREAT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"KEYS">>, args = [Pattern]}) -> C#edis_command{args = [edis_util:glob_to_re(Pattern)], result_type = multi_bulk, group=keys};
parse_command(#edis_command{cmd = <<"KEYS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"MOVE">>, args = [Key, Db]}) ->
  case {edis_util:binary_to_integer(Db, 0), edis_config:get(databases), C#edis_command.db} of
    {DbIndex, Dbs, _} when DbIndex < 0 orelse DbIndex > Dbs -> throw({out_of_range, "index"});
    {CurrentDb, _, CurrentDb} -> throw(source_equals_destination);
    {DbIndex, _, _} -> C#edis_command{args = [Key, DbIndex],result_type = boolean, group=keys}
  end;
parse_command(#edis_command{cmd = <<"MOVE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"OBJECT">>, args = [SubCommand | Rest]}) ->
  parse_command(C#edis_command{cmd = <<"OBJECT ", (edis_util:upper(SubCommand))/binary>>, args = Rest});
parse_command(C = #edis_command{cmd = <<"OBJECT REFCOUNT">>, args = [_Key]}) -> 
  C#edis_command{result_type = number, group=keys};
parse_command(C = #edis_command{cmd = <<"OBJECT ENCODING">>, args = [_Key]}) -> 
  C#edis_command{result_type = bulk, group=keys};
parse_command(C = #edis_command{cmd = <<"OBJECT IDLETIME">>, args = [_Key]}) -> 
  C#edis_command{result_type = number, group=keys};
parse_command(#edis_command{cmd = <<"OBJECT", _Rest/binary>>}) ->
  throw({error, "Syntax error. Try OBJECT (refcount|encoding|idletime)"});
parse_command(C = #edis_command{cmd = <<"PERSIST">>, args = [_Key]}) -> 
  C#edis_command{result_type = boolean, group=keys};
parse_command(#edis_command{cmd = <<"PERSIST">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RANDOMKEY">>, args = []}) -> 
  C#edis_command{result_type = bulk, group=keys};
parse_command(#edis_command{cmd = <<"RANDOMKEY">>}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"RENAME">>, args = [Key, Key]}) -> throw(source_equals_destination);
parse_command(C = #edis_command{cmd = <<"RENAME">>, args = [_Key, _NewKey]}) -> 
  C#edis_command{result_type = ok, group=keys};
parse_command(#edis_command{cmd = <<"RENAME">>}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"RENAMENX">>, args = [Key, Key]}) -> throw(source_equals_destination);
parse_command(C = #edis_command{cmd = <<"RENAMENX">>, args = [_Key, _NewKey]}) -> 
  C#edis_command{result_type = boolean, group=keys};
parse_command(#edis_command{cmd = <<"RENAMENX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SORT">>, args = [Key | Options]}) ->
  SortOptions = parse_sort_options(Options),
  C#edis_command{args = [Key, SortOptions], result_type = sort, group=keys};
parse_command(#edis_command{cmd = <<"SORT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"TTL">>, args =[_Key]}) -> 
  C#edis_command{result_type = number, group=keys};
parse_command(#edis_command{cmd = <<"TTL">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"TYPE">>, args = [_Key]}) -> 
  C#edis_command{result_type = string, group=keys};
parse_command(#edis_command{cmd = <<"TYPE">>}) -> throw(bad_arg_num);
%% -- Hashes ---------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"HDEL">>, args = [_Key, _Field | _Fields]}) -> 
  C#edis_command{result_type = number, group=hashes};
parse_command(#edis_command{cmd = <<"HDEL">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HEXISTS">>, args = [_Key, _Field]}) -> 
  C#edis_command{result_type = boolean, group=hashes};
parse_command(#edis_command{cmd = <<"HEXISTS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HGET">>, args = [_Key, _Field]}) -> 
  C#edis_command{result_type = bulk, group=hashes};
parse_command(#edis_command{cmd = <<"HGET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HGETALL">>, args = [_Key]}) -> 
  C#edis_command{result_type = multi_bulk, group=hashes};
parse_command(#edis_command{cmd = <<"HGETALL">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HINCRBY">>, args = [Key, Field, Increment]}) -> 
  C#edis_command{args = [Key, Field , edis_util:binary_to_integer(Increment)], result_type = number, group=hashes};
parse_command(#edis_command{cmd = <<"HINCRBY">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HKEYS">>, args = [_Key]}) -> 
  C#edis_command{result_type = multi_bulk, group=hashes};
parse_command(#edis_command{cmd = <<"HKEYS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HLEN">>, args = [_Key]}) -> 
  C#edis_command{result_type = number, group=hashes};
parse_command(#edis_command{cmd = <<"HLEN">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HMGET">>, args = [_Key, _Field | _Fields]}) -> 
  C#edis_command{result_type = multi_bulk, group=hashes};
parse_command(#edis_command{cmd = <<"HMGET">>}) -> throw({bad_arg_num, "HMGET"});
parse_command(C = #edis_command{cmd = <<"HMSET">>, args = [Key | FVs]}) when FVs =/= [], length(FVs) rem 2 =:= 0 -> 
  C#edis_command{args = [Key, edis_util:make_pairs(FVs)], result_type = ok, group=hashes};
parse_command(#edis_command{cmd = <<"HMSET">>}) -> throw({bad_arg_num, "HMSET"});
parse_command(C = #edis_command{cmd = <<"HSET">>, args = [_Key, _Field, _Value]}) -> 
  C#edis_command{result_type = boolean, group=hashes};
parse_command(#edis_command{cmd = <<"HSET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HSETNX">>, args = [_Key, _Field, _Value]}) -> 
  C#edis_command{result_type = boolean, group=hashes};
parse_command(#edis_command{cmd = <<"HSETNX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HVALS">>, args = [_Key]}) -> 
  C#edis_command{result_type = multi_bulk, group=hashes};
parse_command(#edis_command{cmd = <<"HVALS">>}) -> throw(bad_arg_num);
%% -- Lists ----------------------------------------------------------------------------------------
parse_command(#edis_command{cmd = <<"BRPOP">>, args = []}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"BRPOP">>, args = [_]}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"BRPOP">>, args = Args}) ->
  [Timeout | Keys] = lists:reverse(Args),
  try edis_util:binary_to_integer(Timeout) of
    T when T < 0 -> throw({is_negative, "timeout"});
    0 -> C#edis_command{args = lists:reverse(Keys), timeout = infinity, expire = never, result_type = multi_bulk, group=lists};
    T -> C#edis_command{args = lists:reverse(Keys), timeout = T * 1000, expire = timeout_to_seconds(T), result_type = multi_bulk, group=lists}
  catch
    _:not_integer -> throw({not_integer, <<"timeout">>})
  end;
parse_command(#edis_command{cmd = <<"BLPOP">>, args = []}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"BLPOP">>, args = [_]}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"BLPOP">>, args = Args}) ->
  [Timeout | Keys] = lists:reverse(Args),
  try edis_util:binary_to_integer(Timeout) of
    T when T < 0 -> throw({is_negative, "timeout"});
    0 -> C#edis_command{args = lists:reverse(Keys), timeout = infinity, expire = never, result_type = multi_bulk, group=lists};
    T -> C#edis_command{args = lists:reverse(Keys), timeout = T * 1000, expire = timeout_to_seconds(T), result_type = multi_bulk, group=lists}
  catch
    _:not_integer -> throw({not_integer, <<"timeout">>})
  end;
parse_command(C = #edis_command{cmd = <<"BRPOPLPUSH">>, args = [Source, Destination, Timeout]}) ->
  try edis_util:binary_to_integer(Timeout) of
    T when T < 0 -> throw({is_negative, "timeout"});
    0 -> C#edis_command{args = [Source, Destination], timeout = infinity, expire = never, result_type = bulk, group=lists};
    T -> C#edis_command{args = [Source, Destination], timeout = T * 1000, expire = timeout_to_seconds(T), result_type = bulk, group=lists}
  catch
    _:not_integer -> throw({not_integer, <<"timeout">>})
  end;
parse_command(#edis_command{cmd = <<"BRPOPLPUSH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LINDEX">>, args = [Key, Index]}) -> 
  C#edis_command{args = [Key, edis_util:binary_to_integer(Index, 0)],result_type = bulk, group=lists};
parse_command(#edis_command{cmd = <<"LINDEX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LINSERT">>, args = [Key, Position, Pivot, Value]}) ->
  case edis_util:upper(Position) of
    <<"BEFORE">> -> C#edis_command{args = [Key, before, Pivot, Value], result_type = number, group=lists};
    <<"AFTER">> -> C#edis_command{args = [Key, 'after', Pivot, Value], result_type = number, group=lists};
    _ -> throw(syntax)
  end;
parse_command(#edis_command{cmd = <<"LINSERT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LLEN">>, args = [_Key]}) -> 
  C#edis_command{result_type = number, group=lists};
parse_command(#edis_command{cmd = <<"LLEN">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LPOP">>, args = [_Key]}) ->
  C#edis_command{result_type = bulk, group=lists};
parse_command(#edis_command{cmd = <<"LPOP">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LPUSH">>, args = [_Key, _Value | _Values]}) -> 
  C#edis_command{result_type = number, group=lists};
parse_command(#edis_command{cmd = <<"LPUSH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LPUSHX">>, args = [_Key, _Value]}) -> 
  C#edis_command{result_type = number, group=lists};
parse_command(#edis_command{cmd = <<"LPUSHX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LRANGE">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0)],result_type = multi_bulk, group=lists};
parse_command(#edis_command{cmd = <<"LRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LREM">>, args = [Key, Count, Value]}) -> 
  C#edis_command{args = [Key, edis_util:binary_to_integer(Count, 0), Value], result_type = number, group=lists};
parse_command(#edis_command{cmd = <<"LREM">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LSET">>, args = [Key, Index, Value]}) -> 
  C#edis_command{args = [Key, edis_util:binary_to_integer(Index, 0), Value], result_type = ok, group=lists};
parse_command(#edis_command{cmd = <<"LSET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LTRIM">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start), edis_util:binary_to_integer(Stop)], result_type = ok, group=lists};
parse_command(#edis_command{cmd = <<"LTRIM">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RPOP">>, args = [_Key]}) -> C#edis_command{result_type = bulk, group=lists};
parse_command(#edis_command{cmd = <<"RPOP">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RPOPLPUSH">>, args = [_Source, _Destination]}) -> 
  C#edis_command{result_type = bulk, group=lists};
parse_command(#edis_command{cmd = <<"RPOPLPUSH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RPUSH">>, args = [_Key, _Value | _Values]}) -> 
  C#edis_command{result_type = number, group=lists};
parse_command(#edis_command{cmd = <<"RPUSH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RPUSHX">>, args = [_Key, _Value]}) -> 
  C#edis_command{result_type = number, group=lists};
parse_command(#edis_command{cmd = <<"RPUSHX">>}) -> throw(bad_arg_num);
%% -- Sets -----------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"SADD">>, args = [_Key, _Member | _Members]}) -> 
  C#edis_command{result_type = number, group=sets};
parse_command(#edis_command{cmd = <<"SADD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SCARD">>, args = [_Key]}) -> 
  C#edis_command{result_type = number, group=sets};
parse_command(#edis_command{cmd = <<"SCARD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SDIFF">>, args = [_Key|_Keys]}) -> 
  C#edis_command{result_type = multi_bulk, group=sets};
parse_command(#edis_command{cmd = <<"SDIFF">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SDIFFSTORE">>, args = [_Destination, _Key | _Keys]}) -> 
  C#edis_command{result_type = number, group=sets};
parse_command(#edis_command{cmd = <<"SDIFFSTORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SINTER">>, args = [_Key|_Keys]}) -> 
  C#edis_command{result_type = multi_bulk, group=sets};
parse_command(#edis_command{cmd = <<"SINTER">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SINTERSTORE">>, args = [_Destination, _Key | _Keys]}) -> 
  C#edis_command{result_type = number, group=sets};
parse_command(#edis_command{cmd = <<"SINTERSTORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SISMEMBER">>, args = [_Key, _Member]}) -> 
  C#edis_command{result_type = boolean, group=sets};
parse_command(#edis_command{cmd = <<"SISMEMBER">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SMEMBERS">>, args = [_Key]}) -> 
  C#edis_command{result_type = multi_bulk, group=sets};
parse_command(#edis_command{cmd = <<"SMEMBERS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SMOVE">>, args = [_Source, _Destination, _Member]}) -> 
  C#edis_command{result_type = boolean, group=sets};
parse_command(#edis_command{cmd = <<"SMOVE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SPOP">>, args = [_Key]}) -> 
  C#edis_command{result_type = bulk, group=sets};
parse_command(#edis_command{cmd = <<"SPOP">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SRANDMEMBER">>, args = [_Key]}) -> 
  C#edis_command{result_type = bulk, group=sets};
parse_command(#edis_command{cmd = <<"SRANDMEMBER">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SREM">>, args = [_Key, _Member | _Members]}) -> 
  C#edis_command{result_type = number, group=sets};
parse_command(#edis_command{cmd = <<"SREM">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SUNION">>, args = [_Key|_Keys]}) -> 
  C#edis_command{result_type = multi_bulk, group=sets};
parse_command(#edis_command{cmd = <<"SUNION">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SUNIONSTORE">>, args = [_Destination, _Key | _Keys]}) -> 
  C#edis_command{result_type = number, group=sets};
parse_command(#edis_command{cmd = <<"SUNIONSTORE">>}) -> throw(bad_arg_num);
%% -- Sets -----------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"ZADD">>, args = [Key | SMs]}) when SMs =/= [], length(SMs) rem 2 =:= 0 ->
  ParsedSMs = [{edis_util:binary_to_float(S), M} || {S, M} <- edis_util:make_pairs(SMs)],
  C#edis_command{args = [Key, ParsedSMs], result_type = number, group= zsets};
%% Redis returns 'ERR syntax error' when receives zadd command with odd arguments bigger than one
parse_command(#edis_command{cmd = <<"ZADD">>, args = [_Key | SMs]}) when SMs =/= [], length(SMs) > 1,length(SMs) rem 2 =:= 1 ->
	throw(syntax);
parse_command(#edis_command{cmd = <<"ZADD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZCARD">>, args = [_Key]}) -> 
  C#edis_command{result_type=number, group=zsets};
parse_command(#edis_command{cmd = <<"ZCARD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZCOUNT">>, args = [Key, Min, Max]}) -> 
  try
    C#edis_command{args = [Key, parse_float_limit(Min), parse_float_limit(Max)],result_type=number, group=zsets}
  catch
    _:not_float -> throw({not_float,"min or max"})
  end;
parse_command(#edis_command{cmd = <<"ZCOUNT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZINCRBY">>, args = [Key, Increment, Member]}) -> 
  C#edis_command{args = [Key, edis_util:binary_to_float(Increment), Member],result_type=float, group=zsets};
parse_command(#edis_command{cmd = <<"ZINCRBY">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZINTERSTORE">>, args = [_Destination, _NumKeys , _FirstKey | _Rest]}) -> parse_zstore_command(C);
parse_command(#edis_command{cmd = <<"ZINTERSTORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZRANGE">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start), edis_util:binary_to_integer(Stop)],result_type=multi_bulk, group=zsets};
parse_command(C = #edis_command{cmd = <<"ZRANGE">>, args = [Key, Start, Stop, Option]}) ->
  case edis_util:upper(Option) of
    <<"WITHSCORES">> -> 
    C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0), with_scores],result_type=multi_bulk, group=zsets};
    _ -> throw(syntax)
  end;
parse_command(#edis_command{cmd = <<"ZRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZRANGEBYSCORE">>, args = [_Key, _Min, _Max | Options]}) when 0 =< length(Options), length(Options) =< 4->
  parse_zrange_command(C);
parse_command(#edis_command{cmd = <<"ZRANGEBYSCORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZRANK">>, args = [_Key, _Member]}) -> 
  C#edis_command{result_type=number, group=zsets};
parse_command(#edis_command{cmd = <<"ZRANK">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREM">>, args = [_Key, _Member | _Members]}) -> 
  C#edis_command{result_type=number, group=zsets};
parse_command(#edis_command{cmd = <<"ZREM">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREMRANGEBYRANK">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start), edis_util:binary_to_integer(Stop)],result_type=number,group=zsets};
parse_command(#edis_command{cmd = <<"ZREMRANGEBYRANK">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREMRANGEBYSCORE">>, args = [Key, Min, Max]}) -> 
  try
    C#edis_command{args = [Key, parse_float_limit(Min), parse_float_limit(Max)],result_type=number,group=zsets}
  catch
    _:not_float -> throw({not_float,"min or max"})
    end;
parse_command(#edis_command{cmd = <<"ZREMRANGEBYSCORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREVRANGE">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0)],result_type=multi_bulk,group=zsets};
parse_command(C = #edis_command{cmd = <<"ZREVRANGE">>, args = [Key, Start, Stop, Option]}) ->
  case edis_util:upper(Option) of
    <<"WITHSCORES">> ->
      C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0), with_scores],result_type=multi_bulk, group=zsets};
    _ -> throw(syntax)
  end;
parse_command(#edis_command{cmd = <<"ZREVRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREVRANGEBYSCORE">>, args = [_Key, _Min, _Max | Options]}) when 0 =< length(Options), length(Options) =< 4->
  parse_zrange_command(C);
parse_command(#edis_command{cmd = <<"ZREVRANGEBYSCORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREVRANK">>, args = [_Key, _Member]}) -> 
  C#edis_command{result_type=number, group=zsets};
parse_command(#edis_command{cmd = <<"ZREVRANK">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZSCORE">>, args = [_Key, _Member]}) -> 
  C#edis_command{result_type=float, group=zsets};
parse_command(#edis_command{cmd = <<"ZSCORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZUNIONSTORE">>, args = [_Destination, _NumKeys, _FirstKey | _Rest ]}) -> parse_zstore_command(C);
parse_command(#edis_command{cmd = <<"ZUNIONSTORE">>}) -> throw(bad_arg_num);
%% -- Server ---------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"CONFIG">>, args = [SubCommand | Rest]}) ->
  parse_command(C#edis_command{cmd = <<"CONFIG ", (edis_util:upper(SubCommand))/binary>>, args = Rest});
parse_command(#edis_command{cmd = <<"CONFIG">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"CONFIG GET">>, args = [Pattern]}) -> 
  C#edis_command{args = [edis_util:glob_to_re(Pattern)],result_type=multi_bulk,group=server};
parse_command(#edis_command{cmd = <<"CONFIG GET">>}) -> throw({bad_arg_num, "CONFIG GET"});
parse_command(C = #edis_command{cmd = <<"CONFIG SET">>, args = [Key | Values]}) ->
  Param = binary_to_atom(edis_util:lower(Key), utf8),
  Value =
    case {Param, Values} of
      {listener_port_range, [P1, P2]} ->
        {edis_util:binary_to_integer(P1),
         edis_util:binary_to_integer(P2)};
      {listener_port_range, _} ->
        throw({bad_arg_num, "CONFIG SET"});
      {client_timeout, [Timeout]} ->
        edis_util:binary_to_integer(Timeout);
      {client_timeout, _} ->
        throw({bad_arg_num, "CONFIG SET"});
      {databases, [Dbs]} ->
        edis_util:binary_to_integer(Dbs);
      {databases, _} ->
        throw({bad_arg_num, "CONFIG SET"});
      {requirepass, []} ->
        undefined;
      {requirepass, [Pass]} ->
        Pass;
      {requirepass, _} ->
        throw({bad_arg_num, "CONFIG SET"});
      {Param, [V]} ->
        V;
      {Param, Vs} ->
        Vs
    end,
  C#edis_command{args = [Param, Value],result_type=ok,group=server};
parse_command(#edis_command{cmd = <<"CONFIG SET">>}) -> throw({bad_arg_num, "CONFIG SET"});
parse_command(C = #edis_command{cmd = <<"CONFIG RESETSTAT">>, args = []}) -> C#edis_command{result_type=ok,group=server};
parse_command(#edis_command{cmd = <<"CONFIG RESETSTAT">>}) -> throw({bad_arg_num, "CONFIG RESETSTAT"});
parse_command(C = #edis_command{cmd = <<"DBSIZE">>, args = []}) -> C#edis_command{result_type=number,group=server};
parse_command(#edis_command{cmd = <<"DBSIZE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"FLUSHALL">>, args = []}) ->  C#edis_command{result_type=ok,group=server};
parse_command(#edis_command{cmd = <<"FLUSHALL">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"FLUSHDB">>, args = []}) ->  C#edis_command{result_type=ok,group=server};
parse_command(#edis_command{cmd = <<"FLUSHDB">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"INFO">>, args = []}) -> C#edis_command{result_type=bulk,group=server};
parse_command(#edis_command{cmd = <<"INFO">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LASTSAVE">>, args = []}) -> C#edis_command{result_type=number,group=server};
parse_command(#edis_command{cmd = <<"LASTSAVE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"MONITOR">>, args = []}) -> C#edis_command{result_type=ok,group=server};
parse_command(#edis_command{cmd = <<"MONITOR">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SAVE">>, args = []}) -> C#edis_command{result_type=ok,group=server};
parse_command(#edis_command{cmd = <<"SAVE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SHUTDOWN">>, args = []}) -> C#edis_command{result_type=ok,group=server};
parse_command(#edis_command{cmd = <<"SHUTDOWN">>}) -> throw(bad_arg_num);
%% -- Pub/Sub --------------------------------------------------------------------------------------
parse_command(#edis_command{cmd = <<"PSUBSCRIBE">>, args = []}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"PSUBSCRIBE">>, args = Patterns}) ->
  C#edis_command{args = lists:map(fun edis_util:glob_to_re/1, Patterns), result_type=number,group=pubsub};
parse_command(C = #edis_command{cmd = <<"PUBLISH">>, args = [_Channel, _Message]}) -> C#edis_command{result_type=number,group=pubsub};
parse_command(#edis_command{cmd = <<"PUBLISH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"PUNSUBSCRIBE">>, args = Patterns}) ->
  C#edis_command{args = lists:map(fun edis_util:glob_to_re/1, Patterns), result_type=number,group=pubsub};
parse_command(#edis_command{cmd = <<"SUBSCRIBE">>, args = []}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SUBSCRIBE">>}) -> C#edis_command{result_type=number,group=pubsub};
parse_command(C = #edis_command{cmd = <<"UNSUBSCRIBE">>}) -> C#edis_command{result_type=number,group=pubsub};
%% -- Transactions ---------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"MULTI">>, args = []}) -> C#edis_command{result_type=ok,group=transaction};
parse_command(#edis_command{cmd = <<"MULTI">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"DISCARD">>, args = []}) -> C#edis_command{result_type=ok,group=transaction};
parse_command(#edis_command{cmd = <<"DISCARD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"EXEC">>, args = []}) -> C#edis_command{result_type=multi_result,group=transaction};
parse_command(#edis_command{cmd = <<"EXEC">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"WATCH">>, args = [_Key|_Keys]}) -> C#edis_command{result_type=ok,group=transaction};
parse_command(#edis_command{cmd = <<"WATCH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"UNWATCH">>, args = []}) -> C#edis_command{result_type=ok,group=transaction};
parse_command(#edis_command{cmd = <<"UNWATCH">>}) -> throw(bad_arg_num);
%% -- Errors ---------------------------------------------------------------------------------------
parse_command(#edis_command{cmd = <<"SYNC">>}) -> throw(unsupported);
parse_command(#edis_command{cmd = <<"SLOWLOG">>}) -> throw(unsupported);
parse_command(#edis_command{cmd = <<"SLAVEOF">>}) -> throw(unsupported);
parse_command(_Command) -> throw(unknown_command).

-spec run(#edis_command{}, state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
%% -- Commands that don't require authorization ----------------------------------------------------
run(#edis_command{cmd = <<"QUIT">>}, State) ->
  case tcp_ok(State) of
    {noreply, NewState, hibernate} -> {stop, normal, NewState};
    Error -> Error
  end;
run(#edis_command{cmd = <<"AUTH">>, args = [Password]}, State) ->
  case edis_config:get(requirepass) of
    undefined -> throw(auth_not_allowed);
    Password -> tcp_ok(State#state{authenticated = true});
    _ -> throw(invalid_password)
  end;
%% -- Authorization --------------------------------------------------------------------------------
run(_, #state{authenticated = false}) -> throw(unauthorized);
%% -- Connection commands that must be run outside the scope of the current db ---------------------
run(#edis_command{cmd = <<"SHUTDOWN">>}, State) ->
  _ = spawn(edis, stop, []),
  {stop, normal, State};
run(#edis_command{cmd = <<"SELECT">>, args = [DbIndex]}, State) ->
  tcp_ok(State#state{db = edis_db:process(DbIndex), db_index = DbIndex});
run(#edis_command{cmd = <<"CONFIG GET">>, args = [Pattern]}, State) ->
  Configs = edis_config:get(Pattern),
  Lines = lists:flatten(
            [[atom_to_binary(K, utf8),
              case V of
                undefined -> undefined;
                V when is_binary(V) -> V;
                V -> erlang:iolist_to_binary(io_lib:format("~p", [V]))
              end] || {K, V} <- Configs]),
  tcp_multi_bulk(Lines, State);
run(#edis_command{cmd = <<"CONFIG SET">>, args = [Param, Value]}, State) ->
  try edis_config:set(Param, Value) of
    ok -> tcp_ok(State)
  catch
    _:invalid_param ->
      throw({error, io_lib:format("Invalid argument '~p' for CONFIG SET '~p'", [Value, Param])});
    _:unsupported_param ->
      throw({error, io_lib:format("Unsupported CONFIG parameter '~p'", [Param])})
  end;
run(#edis_command{cmd = <<"CONFIG RESETSTAT">>}, State) ->
  %%TODO: Reset the statistics
  tcp_ok(State);
run(#edis_command{cmd = <<"FLUSHALL">>}, State) ->
  lists:foreach(
    fun edis_db:flush/1,
    [edis_db:process(Index) || Index <- lists:seq(0, edis_config:get(databases) - 1)]),
  tcp_ok(State);
run(#edis_command{cmd = <<"MONITOR">>}, State) ->
  ok = edis_db_monitor:add_sup_handler(),
  tcp_ok(State);
%% -- Transaction commands -------------------------------------------------------------------------
run(#edis_command{cmd = <<"MULTI">>}, State) -> tcp_ok(State#state{multi_queue = []});
run(#edis_command{cmd = <<"DISCARD">>}, _State) -> throw(out_of_multi);
run(#edis_command{cmd = <<"EXEC">>}, _State) -> throw(out_of_multi);
run(C = #edis_command{cmd = <<"WATCH">>, args = Keys}, State) ->
  NewWatchedKeys =
      lists:foldl(
        fun(Key, WatchedKeys) ->
                case lists:keymember(Key, 1, WatchedKeys) of
                  true -> WatchedKeys;
                  false ->
                    LastUpdate =
                        edis_db:run(State#state.db,
                                    C#edis_command{cmd = <<"OBJECT LASTUPDATE">>, args = [Key],
                                                   result_type = number}),
                    [{Key, LastUpdate}|WatchedKeys]
                end
        end, State#state.watched_keys, Keys),
  tcp_ok(State#state{watched_keys = NewWatchedKeys});
run(#edis_command{cmd = <<"UNWATCH">>, args = []}, State) ->
  tcp_ok(State#state{watched_keys = []});
%% -- Pub/Sub commands -----------------------------------------------------------------------------
run(C = #edis_command{cmd = <<"PSUBSCRIBE">>}, State) ->
  ok = edis_pubsub:add_sup_handler(),
  pubsub(C, State#state{subscriptions = {gb_sets:empty(), gb_sets:empty()}});
run(#edis_command{cmd = <<"PUBLISH">>, args = [Channel, Message]}, State) ->
  ok = edis_pubsub:notify(#edis_message{channel = Channel, message = Message}),
  tcp_number(edis_pubsub:count_handlers(), State);
run(#edis_command{cmd = <<"PUNSUBSCRIBE">>}, _State) -> throw(out_of_pubsub);
run(C = #edis_command{cmd = <<"SUBSCRIBE">>}, State) ->
  ok = edis_pubsub:add_sup_handler(),
  pubsub(C, State#state{subscriptions = {gb_sets:empty(), gb_sets:empty()}});
run(#edis_command{cmd = <<"UNSUBSCRIBE">>}, _State) -> throw(out_of_pubsub);
%% -- All the other commands -----------------------------------------------------------------------
run(C = #edis_command{result_type = ResType, timeout = Timeout}, State) ->
  Res = case Timeout of
          undefined -> edis_db:run(State#state.db, C);
          Timeout -> edis_db:run(State#state.db, C, Timeout)
        end,
  case ResType of
    ok -> tcp_ok(State);
    string -> tcp_string(Res, State);
    bulk -> tcp_bulk(Res, State);
    multi_bulk -> tcp_multi_bulk(Res, State);
    number -> tcp_number(Res, State);
    boolean -> tcp_boolean(Res, State);
    float -> tcp_float(Res, State);
    sort -> tcp_sort(Res, State);
    zrange ->
      [_Key, _Min, _Max, ShowScores, Limit] = C#edis_command.args,
      tcp_zrange(Res, ShowScores, Limit, State)
  end.

-spec queue(#edis_command{}, state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
queue(#edis_command{cmd = <<"QUIT">>}, State) -> %% User may quit even on MULTI block
  case tcp_ok(State) of
    {noreply, NewState, hibernate} -> {stop, normal, NewState};
    Error -> Error
  end;
queue(#edis_command{cmd = <<"MULTI">>}, _State) -> throw(nested);
queue(#edis_command{cmd = <<"WATCH">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"UNWATCH">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"AUTH">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"SHUTDOWN">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"CONFIG", _/binary>>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"SELECT">>}, _State) -> throw(db_in_multi);
queue(#edis_command{cmd = <<"FLUSHALL">>}, _State) -> throw(db_in_multi);
queue(#edis_command{cmd = <<"MOVE">>}, _State) -> throw(db_in_multi);
queue(#edis_command{cmd = <<"MONITOR">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"PUBLISH">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"SUBSCRIBE">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"UNSUBSCRIBE">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"PSUBSCRIBE">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"PUNSUBSCRIBE">>}, _State) -> throw(not_in_multi);
queue(#edis_command{cmd = <<"DISCARD">>}, State) ->
  tcp_ok(State#state{multi_queue = undefined});
queue(C = #edis_command{cmd = <<"EXEC">>}, State) ->
  case lists:any(
         fun({Key, LastUpdate}) ->
                 LastUpdate =/=
                     edis_db:run(State#state.db,
                                 C#edis_command{cmd = <<"OBJECT LASTUPDATE">>, args = [Key],
                                                result_type = number})
         end, State#state.watched_keys) of
    true ->
      tcp_bulk(undefined, State#state{multi_queue = undefined, watched_keys = []});
    false ->
      Commands = lists:reverse(State#state.multi_queue),
      Replies = edis_db:run(State#state.db, C#edis_command{args = Commands}),
      Results =
          lists:zipwith(
            fun(IC, {error, Error}) -> {error, parse_error(IC#edis_command.cmd, Error)};
               (IC, {ok, Reply}) -> {IC, Reply}
            end, Commands, Replies),
      tcp_multi_result(Results, State#state{multi_queue = undefined, watched_keys = []})
  end;
queue(C, State) ->
  tcp_string("QUEUED", State#state{multi_queue = [C|State#state.multi_queue]}).

-spec pubsub(#edis_command{}, state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
pubsub(#edis_command{cmd = <<"QUIT">>}, State) -> %% User may quit even on PUBSUB block
  case tcp_ok(State) of
    {noreply, NewState, hibernate} -> {stop, normal, NewState};
    Error -> Error
  end;
pubsub(#edis_command{cmd = <<"PSUBSCRIBE">>, args = Patterns}, State) ->
  Subscriptons =
    lists:foldl(
      fun(Pattern, {AccChannelSet, AccPatternSet}) ->
              NextPatternSet = gb_sets:add_element(Pattern, AccPatternSet),
              tcp_multi_bulk([<<"psubscribe">>, Pattern,
                              gb_sets:size(AccChannelSet) + gb_sets:size(NextPatternSet)], State),
              {AccChannelSet, NextPatternSet}
      end, State#state.subscriptions, Patterns),
  {noreply, State#state{subscriptions = Subscriptons}, hibernate};
pubsub(C = #edis_command{cmd = <<"PUNSUBSCRIBE">>, args = []}, State) ->
  {_ChannelSet, PatternSet} = State#state.subscriptions,
  pubsub(C#edis_command{args = gb_sets:to_list(PatternSet)}, State);
pubsub(#edis_command{cmd = <<"PUNSUBSCRIBE">>, args = Patterns}, State) ->
  {ChannelSet, PatternSet} =
    lists:foldl(
      fun(Pattern, {AccChannelSet, AccPatternSet}) ->
              NextPatternSet = gb_sets:del_element(Pattern, AccPatternSet),
              tcp_multi_bulk([<<"punsubscribe">>, Pattern,
                              gb_sets:size(AccChannelSet) + gb_sets:size(NextPatternSet)], State),
              {AccChannelSet, NextPatternSet}
      end, State#state.subscriptions, Patterns),
  case gb_sets:size(ChannelSet) + gb_sets:size(PatternSet) of
    0 ->
      ok = edis_pubsub:delete_handler(),
      {noreply, State#state{subscriptions = undefined}, hibernate};
    _ ->
      {noreply, State#state{subscriptions = {ChannelSet, PatternSet}}, hibernate}
  end;
pubsub(#edis_command{cmd = <<"SUBSCRIBE">>, args = Channels}, State) ->
  Subscriptons =
    lists:foldl(
      fun(Channel, {AccChannelSet, AccPatternSet}) ->
              NextChannelSet = gb_sets:add_element(Channel, AccChannelSet),
              tcp_multi_bulk([<<"subscribe">>, Channel,
                              gb_sets:size(NextChannelSet) + gb_sets:size(AccPatternSet)], State),
              {NextChannelSet, AccPatternSet}
      end, State#state.subscriptions, Channels),
  {noreply, State#state{subscriptions = Subscriptons}, hibernate};
pubsub(C = #edis_command{cmd = <<"UNSUBSCRIBE">>, args = []}, State) ->
  {ChannelSet, _PatternSet} = State#state.subscriptions,
  pubsub(C#edis_command{args = gb_sets:to_list(ChannelSet)}, State);
pubsub(#edis_command{cmd = <<"UNSUBSCRIBE">>, args = Channels}, State) ->
  {ChannelSet, PatternSet} =
    lists:foldl(
      fun(Channel, {AccChannelSet, AccPatternSet}) ->
              NextChannelSet = gb_sets:del_element(Channel, AccChannelSet),
              tcp_multi_bulk([<<"unsubscribe">>, Channel,
                              gb_sets:size(NextChannelSet) + gb_sets:size(AccPatternSet)], State),
              {NextChannelSet, AccPatternSet}
      end, State#state.subscriptions, Channels),
  case gb_sets:size(ChannelSet) + gb_sets:size(PatternSet) of
    0 ->
      ok = edis_pubsub:delete_handler(),
      {noreply, State#state{subscriptions = undefined}, hibernate};
    _ ->
      {noreply, State#state{subscriptions = {ChannelSet, PatternSet}}, hibernate}
  end;
pubsub(_C, _State) -> throw(not_in_pubsub).

%% @private
-spec tcp_multi_result([{edis:result_type() | error, term()} | {error, iodata()}], state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_multi_result(Results, State) ->
  lists:foldl(
    fun({#edis_command{result_type = ok}, _}, {noreply, AccState, hibernate}) -> tcp_ok(AccState);
       ({#edis_command{result_type = string}, Res}, {noreply, AccState, hibernate}) -> tcp_string(Res, AccState);
       ({#edis_command{result_type = bulk}, Res}, {noreply, AccState, hibernate}) -> tcp_bulk(Res, AccState);
       ({#edis_command{result_type = multi_bulk}, Res}, {noreply, AccState, hibernate}) -> tcp_multi_bulk(Res, AccState);
       ({#edis_command{result_type = number}, Res}, {noreply, AccState, hibernate}) -> tcp_number(Res, AccState);
       ({#edis_command{result_type = boolean}, Res}, {noreply, AccState, hibernate}) -> tcp_boolean(Res, AccState);
       ({#edis_command{result_type = float}, Res}, {noreply, AccState, hibernate}) -> tcp_float(Res, AccState);
       ({C = #edis_command{result_type = zrange}, Res}, {noreply, AccState, hibernate}) ->
            [_Key, _Min, _Max, ShowScores, Limit] = C#edis_command.args,
            tcp_zrange(Res, ShowScores, Limit, AccState);
       ({error, Err}, {noreply, AccState, hibernate}) -> tcp_err(Err, AccState);
       (_Result, Error) -> Error
    end, tcp_send(["*", integer_to_list(erlang:length(Results))], State), Results).

%% @private
-spec tcp_boolean(boolean(), state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_boolean(true, State) -> tcp_number(1, State);
tcp_boolean(false, State) -> tcp_number(0, State).

%% @private
-spec tcp_sort(undefined | pos_integer() | [binary()], state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_sort(Number, State) when is_integer(Number) -> tcp_number(Number, State);
tcp_sort(Lines, State) -> tcp_multi_bulk(Lines, State).

%% @private
-spec tcp_multi_bulk(undefined | [binary() | float() | integer()], state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_multi_bulk(undefined, State) ->
  tcp_send("*-1", State);
tcp_multi_bulk(Lines, State) ->
  lists:foldl(
    fun(Float, {noreply, AccState, hibernate}) when is_float(Float) ->
            tcp_float(Float, AccState);
       (Integer, {noreply, AccState, hibernate}) when is_integer(Integer) ->
            tcp_number(Integer, AccState);
       (Line, {noreply, AccState, hibernate}) ->
            tcp_bulk(Line, AccState);
       (_Line, Error) ->
            Error
    end, tcp_send(["*", integer_to_list(erlang:length(Lines))], State), Lines).

%% @private
-spec tcp_bulk(undefined | iodata(), state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_bulk(undefined, State) ->
  tcp_send("$-1", State);
tcp_bulk(<<>>, State) ->
  tcp_send("$0\r\n", State);
tcp_bulk(Message, State) ->
  case tcp_send(["$", integer_to_list(iolist_size(Message))], State) of
    {noreply, NewState, hibernate} -> tcp_send(Message, NewState);
    Error -> Error
  end.

%% @private
-spec tcp_number(undefined | integer(), state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_number(undefined, State) ->
  tcp_bulk(undefined, State);
tcp_number(Number, State) ->
  tcp_send([":", integer_to_list(Number)], State).

%% @private
-spec tcp_float(undefined | float(), state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_float(undefined, State) ->
  tcp_bulk(undefined, State);
tcp_float(?POS_INFINITY, State) ->
		tcp_bulk("inf",State);
tcp_float(?NEG_INFINITY, State) ->
		tcp_bulk("-inf",State);
tcp_float(Float, State) ->
  case erlang:trunc(Float) * 1.0 of
    Float -> tcp_bulk(integer_to_list(erlang:trunc(Float)), State);
    _ -> tcp_bulk(io_lib:format("~.18f", [Float]), State)
  end.

%% @private
-spec tcp_err(binary(), state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_err(Message, State) ->
  tcp_send(["-ERR ", Message], State).

%% @private
-spec tcp_ok(state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_ok(State) ->
  tcp_string("OK", State).

%% @private
-spec tcp_string(binary(), state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_string(Message, State) ->
  tcp_send(["+", Message], State).


%% @private
-spec tcp_send(iodata(), state()) -> {noreply, state(), hibernate} | {stop, normal | {error, term()}, state()}.
tcp_send(Message, State) ->
  lager:debug("~p << ~s~n", [State#state.peerport, Message]),
  try gen_tcp:send(State#state.socket, [Message, "\r\n"]) of
    ok ->
      {noreply, State, hibernate};
    {error, closed} ->
      lager:debug("Connection closed~n", []),
      {stop, normal, State};
    {error, Error} ->
      lager:alert("Couldn't send msg through TCP~n\tError: ~p~n", [Error]),
      {stop, {error, Error}, State}
  catch
    _:{Exception, _} ->
      lager:alert("Couldn't send msg through TCP~n\tError: ~p~n", [Exception]),
      {stop, normal, State};
    _:Exception ->
      lager:alert("Couldn't send msg through TCP~n\tError: ~p~n", [Exception]),
      {stop, normal, State}
  end.

parse_float_limit(<<$(, Rest/binary>>) -> {exc, edis_util:binary_to_float(Rest)};
parse_float_limit(Bin) -> {inc, edis_util:binary_to_float(Bin)}.

parse_zstore_command(C) ->
  [Destination, NumKeys | Rest] = C#edis_command.args,
  NK = edis_util:binary_to_integer(NumKeys, 0),
  {Keys, Extras} =
    case {NK, length(Rest)} of
      {0, _} ->
        throw({error, "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"});
      {NK, _} when NK < 0 ->
        throw({error, ["negative length (", NumKeys, ")"]});
      {NK, RL}  when RL < NK->
        throw(syntax);
      {NK, NK} ->
        {Rest, []};
      {NK, RL} when RL == NK + 1 ->
        throw(syntax); %% Extras should at least have name (weight | aggregate) and a value
      {NK, _} ->
        {lists:sublist(Rest, 1, NK), lists:nthtail(NK, Rest)}
    end,
  {Weights, Aggregate} =
    case lists:map(fun edis_util:upper/1, Extras) of
      [] ->
        {[1.0 || _ <- Keys], sum};
      [<<"AGGREGATE">>, <<"SUM">>] -> {[1.0 || _ <- Keys], sum};
      [<<"AGGREGATE">>, <<"MAX">>] -> {[1.0 || _ <- Keys], max};
      [<<"AGGREGATE">>, <<"MIN">>] -> {[1.0 || _ <- Keys], min};
      [<<"AGGREGATE">>, _] -> throw(syntax);
      [<<"WEIGHTS">> | Rest2] ->
        case {NK, length(Rest2)} of
          {NK, R2L}  when R2L < NK->
            throw(syntax);
          {NK, NK} ->
            {try lists:map(fun edis_util:binary_to_float/1, Rest2)
             catch
               _:not_float ->
                 throw({not_float, "weight value"})
             end, sum};
          {NK, R2L} when R2L == NK + 1 ->
            throw(syntax);
          {NK, R2L} when R2L == NK + 2 ->
            {try lists:map(fun edis_util:binary_to_float/1, lists:sublist(Rest2, 1, NK))
             catch
               _:not_float ->
                 throw({not_float, "weight value"})
             end,
             case lists:nthtail(NK, Rest2) of
               [<<"AGGREGATE">>, <<"SUM">>] -> sum;
               [<<"AGGREGATE">>, <<"MAX">>] -> max;
               [<<"AGGREGATE">>, <<"MIN">>] -> min;
               [<<"AGGREGATE">>, _] -> throw(syntax)
             end};
          {NK, _} ->
            throw(syntax)
        end;
      _ ->
        throw(syntax)
    end,
  C#edis_command{args = [Destination, lists:zip(Keys, Weights), Aggregate], result_type = number, group = zsets}.

parse_zrange_command(C) ->
  [Key, Min, Max | Options] = C#edis_command.args,
  
  {ShowScores, Limit} =
    case lists:map(fun edis_util:upper/1, Options) of
      [] -> {false, undefined};
      [<<"WITHSCORES">>] -> {true, undefined};
      [<<"LIMIT">>, Offset, Count] ->
        {false, {edis_util:binary_to_integer(Offset, 0),
                 edis_util:binary_to_integer(Count, 0)}};
      [<<"WITHSCORES">>, <<"LIMIT">>, Offset, Count] ->
        {true, {edis_util:binary_to_integer(Offset, 0), edis_util:binary_to_integer(Count, 0)}};
      [<<"LIMIT">>, Offset, Count, <<"WITHSCORES">>] ->
        {true, {edis_util:binary_to_integer(Offset, 0), edis_util:binary_to_integer(Count, 0)}};
      _ ->
        throw(syntax)
    end,
  C#edis_command{args = [Key, parse_float_limit(Min), parse_float_limit(Max), ShowScores, Limit], group = zsets, result_type = zrange}.

tcp_zrange(Range, ShowScores, Limit, State) ->
  Reply =
    case {ShowScores, Limit} of
      {false, undefined} ->
        [Member || {_Score, Member} <- Range];
      {true, undefined} ->
        lists:flatten([[Member, Score] || {Score, Member} <- Range]);
      {_, {_Off, 0}} ->
        [];
      {_, {Off, _Lim}} when Off < 0 ->
        [];
      {_, {Off, _Lim}} when Off >= length(Range) ->
        [];
      {false, {Off, Lim}} when Lim < 0 ->
        [Member || {_Score, Member} <- lists:nthtail(Off, Range)];
      {true, {Off, Lim}} when Lim < 0 ->
        lists:flatten([[Member, Score] || {Score, Member} <- lists:nthtail(Off, Range)]);
      {false, {Off, Lim}} ->
        [Member || {_Score, Member} <- lists:sublist(Range, Off+1, Lim)];
      {true, {Off, Lim}} ->
        lists:flatten([[Member, Score] || {Score, Member} <- lists:sublist(Range, Off+1, Lim)])
    end,
  tcp_multi_bulk(Reply, State).

timeout_to_seconds(infinity) -> infinity;
timeout_to_seconds(Timeout) -> edis_util:now() + Timeout.

parse_error(Cmd, unsupported) -> <<Cmd/binary, " unsupported in this version">>;
parse_error(Cmd, nested) -> <<Cmd/binary, " calls can not be nested">>;
parse_error(Cmd, out_of_multi) -> <<Cmd/binary, " without MULTI">>;
parse_error(Cmd, not_in_multi) -> <<Cmd/binary, " inside MULTI is not allowed">>;
parse_error(_Cmd, db_in_multi) -> <<"Transactions may include just one database">>;
parse_error(Cmd, out_of_pubsub) -> <<Cmd/binary, " outside PUBSUB mode is not allowed">>;
parse_error(_Cmd, not_in_pubsub) -> <<"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context">>;
parse_error(Cmd, unknown_command) -> <<"unknown command '", Cmd/binary, "'">>;
parse_error(_Cmd, no_such_key) -> <<"no such key">>;
parse_error(_Cmd, syntax) -> <<"syntax error">>;
parse_error(_Cmd, not_integer) -> <<"value is not an integer or out of range">>;
parse_error(_Cmd, {not_integer, Field}) -> [Field, " is not an integer or out of range"];
parse_error(_Cmd, {not_float, Field}) -> [Field, " is not a double"];
parse_error(_Cmd, {out_of_range, Field}) -> [Field, " is out of range"];
parse_error(_Cmd, {is_negative, Field}) -> [Field, " is negative"];
parse_error(_Cmd, not_float) -> <<"value is not a double">>;
parse_error(_Cmd, bad_item_type) -> <<"Operation against a key holding the wrong kind of value">>;
parse_error(_Cmd, source_equals_destination) -> <<"source and destinantion objects are the same">>;
parse_error(Cmd, bad_arg_num) -> <<"wrong number of arguments for '", Cmd/binary, "' command">>;
parse_error(_Cmd, {bad_arg_num, SubCmd}) -> ["wrong number of arguments for ", SubCmd];
parse_error(_Cmd, unauthorized) -> <<"operation not permitted">>;
parse_error(_Cmd, nan_result) -> <<"resulting score is not a number (NaN)">>;
parse_error(_Cmd, auth_not_allowed) -> <<"Client sent AUTH, but no password is set">>;
parse_error(_Cmd, {error, Reason}) -> Reason;
parse_error(_Cmd, Error) -> io_lib:format("~p", [Error]).

parse_sort_options(Options) ->
  parse_sort_options(lists:map(fun edis_util:upper/1, Options), Options, #edis_sort_options{}).
parse_sort_options([], [], SOptions) ->
  SOptions#edis_sort_options{get = case SOptions#edis_sort_options.get of
                                     [] -> [self];
                                     Patterns -> lists:reverse(Patterns)
                                   end};
parse_sort_options([<<"BY">>, _ | Rest], [_, Pattern | Rest2], SOptions) ->
  parse_sort_options(Rest, Rest2, SOptions#edis_sort_options{by = parse_field_pattern(Pattern)});
parse_sort_options([<<"LIMIT">>, _, _ | Rest], [_, Offset, Count | Rest2], SOptions) ->
  parse_sort_options(
    Rest, Rest2, SOptions#edis_sort_options{limit = {edis_util:binary_to_integer(Offset, 0),
                                                     edis_util:binary_to_integer(Count, 0)}});
parse_sort_options([<<"GET">>, _ | Rest], [_, Pattern | Rest2], SOptions) ->
  parse_sort_options(
    Rest, Rest2,
    SOptions#edis_sort_options{get =
                                   [parse_field_pattern(Pattern)|SOptions#edis_sort_options.get]});
parse_sort_options([<<"ASC">> | Rest], [_ | Rest2], SOptions) ->
  parse_sort_options(Rest, Rest2, SOptions#edis_sort_options{direction = asc});
parse_sort_options([<<"DESC">> | Rest], [_ | Rest2], SOptions) ->
  parse_sort_options(Rest, Rest2, SOptions#edis_sort_options{direction = desc});
parse_sort_options([<<"ALPHA">> | Rest], [_ | Rest2], SOptions) ->
  parse_sort_options(Rest, Rest2, SOptions#edis_sort_options{type = alpha});
parse_sort_options([<<"STORE">>, _ | Rest], [_, Field | Rest2], SOptions) ->
  parse_sort_options(Rest, Rest2, SOptions#edis_sort_options{store_in = Field});
parse_sort_options(_, _, _) -> throw(syntax).

parse_field_pattern(Pattern) ->
  case binary:split(Pattern, <<"->">>) of
    [<<"#">>] -> self;
    [Pattern] -> Pattern;
    [Key, Field] -> {Key, Field}
  end.
