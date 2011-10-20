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

-record(state, {socket                  :: port(),
                db = edis_db:process(0) :: atom(),
                db_index = 0            :: non_neg_integer(),
                peerport                :: pos_integer(),
                authenticated = false   :: boolean(),
                multi_queue = undefined :: undefined | [{binary(), [binary()]}]}).
-opaque state() :: #state{}.

-export([start_link/1, stop/1, err/2, run/3]).
-export([last_arg/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% =================================================================================================
%% External functions
%% =================================================================================================
-spec start_link(port()) -> {ok, pid()}.
start_link(Socket) ->
  gen_server:start_link(?MODULE, Socket, []).

-spec stop(pid()) -> ok.
stop(Runner) ->
  gen_server:cast(Runner, stop).

-spec err(pid(), iodata()) -> ok.
err(Runner, Message) ->
  gen_server:cast(Runner, {err, Message}).

-spec run(pid(), binary(), [binary()]) -> ok.
run(Runner, Command, Arguments) ->
  gen_server:cast(Runner, {run, Command, Arguments}).

%% @doc Should last argument be inlined?
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
    case inet:peername(Socket) of
      {ok, {_Ip, Port}} -> Port;
      Error -> Error
    end,
  Authenticated = undefined =:= edis_config:get(requirepass),
  {ok, #state{socket = Socket, peerport = PeerPort, authenticated = Authenticated}}.

%% @hidden
-spec handle_call(X, reference(), state()) -> {stop, {unexpected_request, X}, {unexpected_request, X}, state()}.
handle_call(X, _From, State) -> {stop, {unexpected_request, X}, {unexpected_request, X}, State}.

%% @hidden
-spec handle_cast(stop | {err, binary()} | {run, binary(), [binary()]}, state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast({err, Message}, State) ->
  tcp_err(Message, State);
handle_cast({run, Cmd, Args}, State) ->
  try
    Command = parse_command(#edis_command{cmd = Cmd,
                                          db = State#state.db_index,
                                          args = Args}),
    run(Command, State)
  catch
    _:unknown_command ->
      ?ERROR("Unknown command ~s.~n", [Cmd]),
      tcp_err(["unknown command '", Cmd, "'"], State);
    _:no_such_key ->
      ?ERROR("No such key for ~s on db #~p~n", [Cmd, State#state.db_index]),
      tcp_err("no such key", State);
    _:syntax ->
      ?ERROR("Syntax error for ~s on db #~p~n", [Cmd, State#state.db_index]),
      tcp_err("syntax error", State);
    _:not_integer ->
      ?ERROR("The value affected by ~s was not a integer on ~p~n", [Cmd, State#state.db_index]),
      tcp_err("value is not an integer or out of range", State);
    _:{not_integer, Field} ->
      ?ERROR("The value affected by ~s's ~s was not a integer on ~p~n", [Cmd, Field, State#state.db_index]),
      tcp_err([Field, " is not an integer or out of range"], State);
    _:{not_float, Field} ->
      ?ERROR("The value affected by ~s's ~s was not a float on ~p~n", [Cmd, Field, State#state.db_index]),
      tcp_err([Field, " is not a double"], State);
    _:{out_of_range, Field} ->
      ?ERROR("The value affected by ~s's ~s was out of range on ~p~n", [Cmd, Field, State#state.db_index]),
      tcp_err([Field, " is out of range"], State);
    _:{is_negative, Field} ->
      ?ERROR("The value affected by ~s's ~s was negative on ~p~n", [Cmd, Field, State#state.db_index]),
      tcp_err([Field, " is negative"], State);
    _:not_float ->
      ?ERROR("The value affected by ~s was not a float on ~p~n", [Cmd, State#state.db_index]),
      tcp_err("value is not a double", State);
    _:bad_item_type ->
      ?ERROR("Bad type running ~s on db #~p~n", [Cmd, State#state.db_index]),
      tcp_err("Operation against a key holding the wrong kind of value", State);
    _:source_equals_destination ->
      tcp_err("source and destinantion objects are the same", State);
    _:bad_arg_num ->
      tcp_err(["wrong number of arguments for '", Cmd, "' command"], State);
    _:{bad_arg_num, SubCmd} ->
      tcp_err(["wrong number of arguments for ", SubCmd], State);
    _:unauthorized ->
      ?WARN("Unauthorized user trying to do a ~s on ~p~n", [Cmd, State#state.db_index]),
      tcp_err("operation not permitted", State);
    _:{error, Reason} ->
      ?ERROR("Error running ~s on db #~p: ~p~n", [Cmd, State#state.db_index, Reason]),
      tcp_err(Reason, State);
    _:Error ->
      ?ERROR("Error running ~s on ~p:~n\t~p~n", [Cmd, State#state.db_index, Error]),
      tcp_err(io_lib:format("~p", [Error]), State)
  end.

%% @hidden
-spec handle_info(term(), state()) -> {noreply, state(), hibernate}.
handle_info(#edis_command{db = 0} = Command, State) ->
  tcp_ok(io_lib:format("~p ~s ~p", [Command#edis_command.timestamp,
                                    Command#edis_command.cmd,
                                    Command#edis_command.args]), State);
handle_info(#edis_command{} = Command, State) ->
  tcp_ok(io_lib:format("~p (db ~p) ~s ~p", [Command#edis_command.timestamp,
                                            Command#edis_command.db,
                                            Command#edis_command.cmd,
                                            Command#edis_command.args]), State);
handle_info({gen_event_EXIT, _Handler, Reason}, State) ->
  ?INFO("Monitor deactivated. Reason: ~p~n", [Reason]),
  {noreply, State, hibernate};
handle_info(Info, State) ->
  ?INFO("Unexpected info: ~p~n", [Info]),
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
parse_command(C = #edis_command{cmd = <<"QUIT">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"QUIT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"AUTH">>, args = [_Password]}) -> C;
parse_command(#edis_command{cmd = <<"AUTH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SELECT">>, args = [Db]}) ->
  try {edis_util:binary_to_integer(Db, 0), edis_config:get(databases)} of
    {DbIndex, Dbs} when DbIndex < 0 orelse DbIndex >= Dbs -> throw({error, "invalid DB index"});
    {DbIndex, _} -> C#edis_command{args = [DbIndex]}
  catch
    error:not_integer ->
      ?WARN("Switching to db 0 because we received '~s' as the db index. This behaviour was copied from redis-server~n", [Db]),
      #edis_command{args = [0]}
  end;
parse_command(#edis_command{cmd = <<"SELECT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"PING">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"PING">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ECHO">>, args = [_Word]}) -> C;
parse_command(#edis_command{cmd = <<"ECHO">>}) -> throw(bad_arg_num);
%% -- Strings --------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"APPEND">>, args = [_Key, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"APPEND">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"DECR">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"DECR">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"DECRBY">>, args = [_Key, _Decrement]}) -> C;
parse_command(#edis_command{cmd = <<"DECRBY">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"GET">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"GET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"GETBIT">>, args = [Key, Offset]}) ->
  try edis_util:binary_to_integer(Offset) of
    O when O >= 0 -> C#edis_command{args = [Key, 0]};
    _ -> throw({not_integer, "bit offset"})
  catch
    _:not_integer -> throw({not_integer, "bit offset"})
  end;
parse_command(#edis_command{cmd = <<"GETBIT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"GETRANGE">>, args = [Key, Start, End]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start), edis_util:binary_to_integer(End)]};
parse_command(#edis_command{cmd = <<"GETRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"GETSET">>, args = [_Key, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"GETSET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"INCR">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"INCR">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"INCRBY">>, args = [Key, Increment]}) -> C#edis_command{args = [Key, edis_util:binary_to_integer(Increment)]};
parse_command(#edis_command{cmd = <<"INCRBY">>}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"MGET">>, args = []}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"MGET">>}) -> C;
parse_command(C = #edis_command{cmd = <<"MSET">>, args = KVs}) when KVs =/= [], length(KVs) rem 2 =:= 0 -> C#edis_command{args = edis_util:make_pairs(KVs)};
parse_command(#edis_command{cmd = <<"MSET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"MSETNX">>, args = KVs}) when KVs =/= [], length(KVs) rem 2 =:= 0 -> C#edis_command{args = edis_util:make_pairs(KVs)};
parse_command(#edis_command{cmd = <<"MSETNX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SET">>, args = [_Key, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"SET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SETBIT">>, args = [Key, Offset, Bit]}) ->
  try {edis_util:binary_to_integer(Offset), Bit} of
    {O, Bit} when O >= 0, Bit == <<"0">> -> C#edis_command{args = [Key, O, 0]};
    {O, Bit} when O >= 0, Bit == <<"1">> -> C#edis_command{args = [Key, O, 1]};
    {O, _BadBit} when O >= 0 -> throw({not_integer, "bit"});
    _ -> throw({not_integer, "bit offset"})
  catch
    _:not_integer -> throw({not_integer, "bit offset"})
  end;
parse_command(#edis_command{cmd = <<"SETBIT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SETEX">>, args = [Key, Seconds, Value]}) ->
  case edis_util:binary_to_integer(Seconds) of
    Secs when Secs =< 0 -> throw({error, "invalid expire time in SETEX"});
    Secs -> C#edis_command{args = [Key, Secs, Value]}
  end;
parse_command(#edis_command{cmd = <<"SETEX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SETNX">>, args = [_Key, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"SETNX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SETRANGE">>, args = [Key, Offset, Value]}) ->
  case edis_util:binary_to_integer(Offset) of
    Off when Off < 0 -> throw({out_of_range, "offset"});
    Off -> C#edis_command{args = [Key, Off, Value]}
  end;
parse_command(#edis_command{cmd = <<"SETRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"STRLEN">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"STRLEN">>}) -> throw(bad_arg_num);
%% -- Keys -----------------------------------------------------------------------------------------
parse_command(#edis_command{cmd = <<"DEL">>, args = []}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"DEL">>}) -> C;
parse_command(C = #edis_command{cmd = <<"EXISTS">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"EXISTS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"EXPIRE">>, args = [Key, Seconds]}) -> C#edis_command{args = [Key, edis_util:binary_to_integer(Seconds)]};
parse_command(#edis_command{cmd = <<"EXPIRE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"EXPIREAT">>, args = [Key, Timestamp]}) -> C#edis_command{args = [Key, edis_util:binary_to_integer(Timestamp)]};
parse_command(#edis_command{cmd = <<"EXPIREAT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"KEYS">>, args = [Pattern]}) -> C#edis_command{args = [edis_util:glob_to_re(Pattern)]};
parse_command(#edis_command{cmd = <<"KEYS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"MOVE">>, args = [Key, Db]}) ->
  case {edis_util:binary_to_integer(Db, 0), edis_config:get(databases), C#edis_command.db} of
    {DbIndex, Dbs, _} when DbIndex < 0 orelse DbIndex > Dbs -> throw({out_of_range, "index"});
    {CurrentDb, _, CurrentDb} -> throw(source_equals_destination);
    {DbIndex, _, _} -> C#edis_command{args = [Key, DbIndex]}
  end;
parse_command(#edis_command{cmd = <<"MOVE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"OBJECT">>, args = [SubCommand | Rest]}) ->
  parse_command(C#edis_command{cmd = <<"OBJECT ", (edis_util:upper(SubCommand))/binary>>, args = Rest});
parse_command(C = #edis_command{cmd = <<"OBJECT REFCOUNT">>, args = [_Key]}) -> C;
parse_command(C = #edis_command{cmd = <<"OBJECT ENCODING">>, args = [_Key]}) -> C;
parse_command(C = #edis_command{cmd = <<"OBJECT IDLETIME">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"OBJECT", _Rest/binary>>}) ->
  throw({error, "Syntax error. Try OBJECT (refcount|encoding|idletime)"});
parse_command(C = #edis_command{cmd = <<"PERSIST">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"PERSIST">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RANDOMKEY">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"RANDOMKEY">>}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"RENAME">>, args = [Key, Key]}) -> throw(source_equals_destination);
parse_command(C = #edis_command{cmd = <<"RENAME">>, args = [_Key, _NewKey]}) -> C;
parse_command(#edis_command{cmd = <<"RENAME">>}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"RENAMENX">>, args = [Key, Key]}) -> throw(source_equals_destination);
parse_command(C = #edis_command{cmd = <<"RENAMENX">>, args = [_Key, _NewKey]}) -> C;
parse_command(#edis_command{cmd = <<"RENAMENX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"TTL">>, args =[_Key]}) -> C;
parse_command(#edis_command{cmd = <<"TTL">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"TYPE">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"TYPE">>}) -> throw(bad_arg_num);
%% -- Hashes ---------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"HDEL">>, args = [_Key, _Field | _Fields]}) -> C;
parse_command(#edis_command{cmd = <<"HDEL">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HEXISTS">>, args = [_Key, _Field]}) -> C;
parse_command(#edis_command{cmd = <<"HEXISTS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HGET">>, args = [_Key, _Field]}) -> C;
parse_command(#edis_command{cmd = <<"HGET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HGETALL">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"HGETALL">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HINCRBY">>, args = [Key, Field, Increment]}) -> C#edis_command{args = [Key, Field , edis_util:binary_to_integer(Increment)]};
parse_command(#edis_command{cmd = <<"HINCRBY">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HKEYS">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"HKEYS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HLEN">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"HLEN">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HMGET">>, args = [_Key, _Field | _Fields]}) -> C;
parse_command(#edis_command{cmd = <<"HMGET">>}) -> throw({bad_arg_num, "HMGET"});
parse_command(C = #edis_command{cmd = <<"HMSET">>, args = [Key | FVs]}) when FVs =/= [], length(FVs) rem 2 =:= 0 -> C#edis_command{args = [Key, edis_util:make_pairs(FVs)]};
parse_command(#edis_command{cmd = <<"HMSET">>}) -> throw({bad_arg_num, "HMSET"});
parse_command(C = #edis_command{cmd = <<"HSET">>, args = [_Key, _Field, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"HSET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HSETNX">>, args = [_Key, _Field, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"HSETNX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"HVALS">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"HVALS">>}) -> throw(bad_arg_num);
%% -- Lists ----------------------------------------------------------------------------------------
parse_command(#edis_command{cmd = <<"BRPOP">>, args = []}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"BRPOP">>, args = [_]}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"BRPOP">>, args = Args}) ->
  [Timeout | Keys] = lists:reverse(Args),
  case edis_util:binary_to_integer(Timeout) of
    T when T < 0 -> throw({is_negative, "timeout"});
    0 -> C#edis_command{args = lists:reverse([infinity | Keys])};
    T -> C#edis_command{args = lists:reverse([T | Keys])}
  end;
parse_command(#edis_command{cmd = <<"BLPOP">>, args = []}) -> throw(bad_arg_num);
parse_command(#edis_command{cmd = <<"BLPOP">>, args = [_]}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"BLPOP">>, args = Args}) ->
  [Timeout | Keys] = lists:reverse(Args),
  case edis_util:binary_to_integer(Timeout) of
    T when T < 0 -> throw({is_negative, "timeout"});
    0 -> C#edis_command{args = lists:reverse([infinity | Keys])};
    T -> C#edis_command{args = lists:reverse([T | Keys])}
  end;
parse_command(C = #edis_command{cmd = <<"BRPOPLPUSH">>, args = [Source, Destination, Timeout]}) ->
  case edis_util:binary_to_integer(Timeout) of
    T when T < 0 -> throw({is_negative, "timeout"});
    0 -> C#edis_command{args = [Source, Destination, infinity]};
    T -> C#edis_command{args = [Source, Destination, T]}
  end;
parse_command(#edis_command{cmd = <<"BRPOPLPUSH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LINDEX">>, args = [Key, Index]}) -> C#edis_command{args = [Key, edis_util:binary_to_integer(Index, 0)]};
parse_command(#edis_command{cmd = <<"LINDEX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LINSERT">>, args = [Key, Position, Pivot, Value]}) ->
  case edis_util:upper(Position) of
    <<"BEFORE">> -> C#edis_command{args = [Key, before, Pivot, Value]};
    <<"AFTER">> -> C#edis_command{args = [Key, 'after', Pivot, Value]};
    _ -> throw(syntax)
  end;
parse_command(#edis_command{cmd = <<"LINSERT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LLEN">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"LLEN">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LPOP">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"LPOP">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LPUSH">>, args = [_Key, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"LPUSH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LPUSHX">>, args = [_Key, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"LPUSHX">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LRANGE">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0)]};
parse_command(#edis_command{cmd = <<"LRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LREM">>, args = [Key, Count, Value]}) -> C#edis_command{args = [Key, edis_util:binary_to_integer(Count, 0), Value]};
parse_command(#edis_command{cmd = <<"LREM">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LSET">>, args = [Key, Index, Value]}) -> C#edis_command{args = [Key, edis_util:binary_to_integer(Index, 0), Value]};
parse_command(#edis_command{cmd = <<"LSET">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LTRIM">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start), edis_util:binary_to_integer(Stop)]};
parse_command(#edis_command{cmd = <<"LTRIM">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RPOP">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"RPOP">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RPOPLPUSH">>, args = [_Source, _Destination]}) -> C;
parse_command(#edis_command{cmd = <<"RPOPLPUSH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RPUSH">>, args = [_Key, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"RPUSH">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"RPUSHX">>, args = [_Key, _Value]}) -> C;
parse_command(#edis_command{cmd = <<"RPUSHX">>}) -> throw(bad_arg_num);
%% -- Sets -----------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"SADD">>, args = [_Key, _Member | _Members]}) -> C;
parse_command(#edis_command{cmd = <<"SADD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SCARD">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"SCARD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SDIFF">>, args = [_Key|_Keys]}) -> C;
parse_command(#edis_command{cmd = <<"SDIFF">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SDIFFSTORE">>, args = [_Destination, _Key | _Keys]}) -> C;
parse_command(#edis_command{cmd = <<"SDIFFSTORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SINTER">>, args = [_Key|_Keys]}) -> C;
parse_command(#edis_command{cmd = <<"SINTER">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SINTERSTORE">>, args = [_Destination, _Key | _Keys]}) -> C;
parse_command(#edis_command{cmd = <<"SINTERSTORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SISMEMBER">>, args = [_Key, _Member]}) -> C;
parse_command(#edis_command{cmd = <<"SISMEMBER">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SMEMBERS">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"SMEMBERS">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SMOVE">>, args = [_Source, _Destination, _Member]}) -> C;
parse_command(#edis_command{cmd = <<"SMOVE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SPOP">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"SPOP">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SRANDMEMBER">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"SRANDMEMBER">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SREM">>, args = [_Key, _Member | _Members]}) -> C;
parse_command(#edis_command{cmd = <<"SREM">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SUNION">>, args = [_Key|_Keys]}) -> C;
parse_command(#edis_command{cmd = <<"SUNION">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SUNIONSTORE">>, args = [_Destination, _Key | _Keys]}) -> C;
parse_command(#edis_command{cmd = <<"SUNIONSTORE">>}) -> throw(bad_arg_num);
%% -- Sets -----------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"ZADD">>, args = [Key | SMs]}) when SMs =/= [], length(SMs) rem 2 =:= 0 ->
  ParsedSMs = [{edis_util:binary_to_float(S), M} || {S, M} <- edis_util:make_pairs(SMs)],
  C#edis_command{args = [Key, ParsedSMs]};
parse_command(#edis_command{cmd = <<"ZADD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZCARD">>, args = [_Key]}) -> C;
parse_command(#edis_command{cmd = <<"ZCARD">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZCOUNT">>, args = [Key, Min, Max]}) -> C#edis_command{args = [Key, parse_float_limit(Min), parse_float_limit(Max)]};
parse_command(#edis_command{cmd = <<"ZCOUNT">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZINCRBY">>, args = [Key, Increment, Member]}) -> C#edis_command{args = [Key, edis_util:binary_to_float(Increment), Member]};
parse_command(#edis_command{cmd = <<"ZINCRBY">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZINTERSTORE">>, args = [_Destination, _NumKeys | _Rest]}) -> parse_zstore_command(C);
parse_command(#edis_command{cmd = <<"ZINTERSTORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZRANGE">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0)]};
parse_command(C = #edis_command{cmd = <<"ZRANGE">>, args = [Key, Start, Stop, Option]}) ->
  case edis_util:upper(Option) of
    <<"WITHSCORES">> -> C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0), with_scores]};
    _ -> throw(syntax)
  end;
parse_command(#edis_command{cmd = <<"ZRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZRANGEBYSCORE">>, args = [_Key, _Min, _Max | Options]}) when 0 =< length(Options), length(Options) =< 4->
  parse_zrange_command(C);
parse_command(#edis_command{cmd = <<"ZRANGEBYSCORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZRANK">>, args = [_Key, _Member]}) -> C;
parse_command(#edis_command{cmd = <<"ZRANK">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREM">>, args = [_Key, _Member | _Members]}) -> C;
parse_command(#edis_command{cmd = <<"ZREM">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREMRANGEBYRANK">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0)]};
parse_command(#edis_command{cmd = <<"ZREMRANGEBYRANK">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREMRANGEBYSCORE">>, args = [Key, Min, Max]}) -> C#edis_command{args = [Key, parse_float_limit(Min), parse_float_limit(Max)]};
parse_command(#edis_command{cmd = <<"ZREMRANGEBYSCORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREVRANGE">>, args = [Key, Start, Stop]}) ->
  C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0)]};
parse_command(C = #edis_command{cmd = <<"ZREVRANGE">>, args = [Key, Start, Stop, Option]}) ->
  case edis_util:upper(Option) of
    <<"WITHSCORES">> -> C#edis_command{args = [Key, edis_util:binary_to_integer(Start, 0), edis_util:binary_to_integer(Stop, 0), with_scores]};
    _ -> throw(syntax)
  end;
parse_command(#edis_command{cmd = <<"ZREVRANGE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREVRANGEBYSCORE">>, args = [_Key, _Min, _Max | Options]}) when 0 =< length(Options), length(Options) =< 4->
  parse_zrange_command(C);
parse_command(#edis_command{cmd = <<"ZREVRANGEBYSCORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZREVRANK">>, args = [_Key, _Member]}) -> C;
parse_command(#edis_command{cmd = <<"ZREVRANK">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZSCORE">>, args = [_Key, _Member]}) -> C;
parse_command(#edis_command{cmd = <<"ZSCORE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"ZUNIONSTORE">>, args = [_Destination, _NumKeys | _Rest]}) -> parse_zstore_command(C);
parse_command(#edis_command{cmd = <<"ZUNIONSTORE">>}) -> throw(bad_arg_num);
%% -- Server ---------------------------------------------------------------------------------------
parse_command(C = #edis_command{cmd = <<"CONFIG">>, args = [SubCommand | Rest]}) ->
  parse_command(C#edis_command{cmd = <<"CONFIG ", (edis_util:upper(SubCommand))/binary>>, args = Rest});
parse_command(#edis_command{cmd = <<"CONFIG">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"CONFIG GET">>, args = [Pattern]}) -> C#edis_command{args = [edis_util:glob_to_re(Pattern)]};
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
      {client_tiemout, _} ->
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
  C#edis_command{args = [Param, Value]};
parse_command(#edis_command{cmd = <<"CONFIG SET">>}) -> throw({bad_arg_num, "CONFIG SET"});
parse_command(C = #edis_command{cmd = <<"CONFIG RESETSTAT">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"CONFIG RESETSTAT">>}) -> throw({bad_arg_num, "CONFIG RESETSTAT"});
parse_command(C = #edis_command{cmd = <<"DBSIZE">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"DBSIZE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"FLUSHALL">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"FLUSHALL">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"FLUSHDB">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"FLUSHDB">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"INFO">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"INFO">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"LASTSAVE">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"LASTSAVE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"MONITOR">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"MONITOR">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SAVE">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"SAVE">>}) -> throw(bad_arg_num);
parse_command(C = #edis_command{cmd = <<"SHUTDOWN">>, args = []}) -> C;
parse_command(#edis_command{cmd = <<"SHUTDOWN">>}) -> throw(bad_arg_num);
%% -- Errors ---------------------------------------------------------------------------------------
parse_command(#edis_command{cmd = <<"SYNC">>}) -> throw({error, "unsupported command"});
parse_command(#edis_command{cmd = <<"SLOWLOG">>}) -> throw({error, "unsupported command"});
parse_command(#edis_command{cmd = <<"SLAVEOF">>}) -> throw({error, "unsupported command"});
parse_command(#edis_command{cmd = <<"SORT">>}) -> throw({error, "unsupported command"});
parse_command(_Command) -> throw(unknown_command).

-spec run(#edis_command{}, state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
%% -- Connection -----------------------------------------------------------------------------------
run(#edis_command{cmd = <<"QUIT">>}, State) ->
  case tcp_ok(State) of
    {noreply, NewState} -> {stop, normal, NewState};
    Error -> Error
  end;
run(#edis_command{cmd = <<"AUTH">>, args = [Password]}, State) ->
  case edis_config:get(requirepass) of
    undefined -> tcp_ok(State);
    Password -> tcp_ok(State#state{authenticated = true});
    _ -> tcp_err(<<"invalid password">>, State#state{authenticated = false})
  end;
run(_, #state{authenticated = false}) -> throw(unauthorized);
run(#edis_command{cmd = <<"SELECT">>, args = [DbIndex]}, State) ->
  tcp_ok(State#state{db = edis_db:process(DbIndex)});
run(#edis_command{cmd = <<"PING">>}, State) ->
  pong = edis_db:ping(State#state.db),
  tcp_ok(<<"PONG">>, State);
run(#edis_command{cmd = <<"ECHO">>, args = [Word]}, State) ->
  tcp_bulk(Word, State);
%% -- Strings --------------------------------------------------------------------------------------
run(#edis_command{cmd = <<"APPEND">>, args = [Key, Value]}, State) ->
  tcp_number(edis_db:append(State#state.db, Key, Value), State);
run(#edis_command{cmd = <<"DECR">>, args = [Key]}, State) ->
  tcp_number(edis_db:decr(State#state.db, Key, 1), State);
run(#edis_command{cmd = <<"DECRBY">>, args = [Key, Decrement]}, State) ->
  tcp_number(edis_db:decr(State#state.db, Key, Decrement), State);
run(#edis_command{cmd = <<"GET">>, args = [Key]}, State) ->
  tcp_bulk(edis_db:get(State#state.db, Key), State);
run(#edis_command{cmd = <<"GETBIT">>, args = [Key, Offset]}, State) ->
  tcp_number(edis_db:get_bit(State#state.db, Key, Offset), State);
run(#edis_command{cmd = <<"GETRANGE">>, args = [Key, Start, End]}, State) ->
  tcp_bulk(edis_db:get_range(State#state.db, Key, Start, End), State);
run(#edis_command{cmd = <<"GETSET">>, args = [Key, Value]}, State) ->
  tcp_bulk(edis_db:get_and_set(State#state.db, Key, Value), State);
run(#edis_command{cmd = <<"INCR">>, args = [Key]}, State) ->
  tcp_number(edis_db:incr(State#state.db, Key, 1), State);
run(#edis_command{cmd = <<"INCRBY">>, args = [Key, Increment]}, State) ->
  tcp_number(edis_db:incr(State#state.db, Key, Increment), State);
run(#edis_command{cmd = <<"MGET">>, args = Keys}, State) ->
  tcp_multi_bulk(edis_db:get(State#state.db, Keys), State);
run(#edis_command{cmd = <<"MSET">>, args = KVs}, State) when KVs =/= [], length(KVs) rem 2 =:= 0 ->
  ok = edis_db:set(State#state.db, KVs),
  tcp_ok(State);
run(#edis_command{cmd = <<"MSETNX">>, args = KVs}, State) when KVs =/= [], length(KVs) rem 2 =:= 0 ->
  try edis_db:set_nx(State#state.db, KVs) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run(#edis_command{cmd = <<"SET">>, args = [Key, Value]}, State) ->
  ok = edis_db:set(State#state.db, Key, Value),
  tcp_ok(State);
run(#edis_command{cmd = <<"SETBIT">>, args = [Key, Offset, Bit]}, State) ->
  tcp_number(edis_db:set_bit(State#state.db, Key, Offset, Bit), State);
run(#edis_command{cmd = <<"SETEX">>, args = [Key, Seconds, Value]}, State) ->
  ok = edis_db:set_ex(State#state.db, Key, Seconds, Value),
  tcp_ok(State);
run(#edis_command{cmd = <<"SETNX">>, args = [Key, Value]}, State) ->
  try edis_db:set_nx(State#state.db, Key, Value) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run(#edis_command{cmd = <<"SETRANGE">>, args = [Key, Offset, Value]}, State) ->
  tcp_number(edis_db:set_range(State#state.db, Key, Offset, Value), State);
run(#edis_command{cmd = <<"STRLEN">>, args = [Key]}, State) ->
  tcp_number(edis_db:str_len(State#state.db, Key), State);
%% -- Keys -----------------------------------------------------------------------------------------
run(#edis_command{cmd = <<"DEL">>, args = Keys}, State) ->
  tcp_number(edis_db:del(State#state.db, Keys), State);
run(#edis_command{cmd = <<"EXISTS">>, args = [Key]}, State) ->
  tcp_boolean(edis_db:exists(State#state.db, Key), State);
run(#edis_command{cmd = <<"EXPIRE">>, args = [Key, Seconds]}, State) ->
  tcp_boolean(edis_db:expire(State#state.db, Key, Seconds), State);
run(#edis_command{cmd = <<"EXPIREAT">>, args = [Key, Timestamp]}, State) ->
  tcp_boolean(edis_db:expire_at(State#state.db, Key, Timestamp), State);
run(#edis_command{cmd = <<"KEYS">>, args = [Pattern]}, State) ->
  tcp_multi_bulk(edis_db:keys(State#state.db, Pattern), State);
run(#edis_command{cmd = <<"MOVE">>, db = CurrentDb, args = [Key, DbIndex]}, State) ->
  tcp_boolean(edis_db:move(CurrentDb, Key, DbIndex), State);
run(#edis_command{cmd = <<"OBJECT REFCOUNT">>, args = [Key]}, State) ->
  %%XXX: Not *really* implemented
  case edis_db:exists(State#state.db, Key) of
    true -> tcp_number(1, State);
    false -> tcp_bulk(undefined, State)
  end;
run(#edis_command{cmd = <<"OBJECT ENCODING">>, args = [Key]}, State) ->
  Reply =
      case edis_db:encoding(State#state.db, Key) of
        undefined -> undefined;
        Encoding -> atom_to_binary(Encoding, utf8)
      end,
  tcp_bulk(Reply, State);
run(#edis_command{cmd = <<"OBJECT IDLETIME">>, args = [Key]}, State) ->
  tcp_number(edis_db:idle_time(State#state.db, Key), State);
run(#edis_command{cmd = <<"PERSIST">>, args = [Key]}, State) ->
  tcp_boolean(edis_db:persist(State#state.db, Key), State);
run(#edis_command{cmd = <<"RANDOMKEY">>}, State) ->
  tcp_bulk(edis_db:random_key(State#state.db), State);
run(#edis_command{cmd = <<"RENAME">>, args = [Key, NewKey]}, State) ->
  try edis_db:rename(State#state.db, Key, NewKey) of
    ok -> tcp_ok(State)
  catch
    _:not_found ->
      throw(no_such_key)
  end;
run(#edis_command{cmd = <<"RENAMENX">>, args = [Key, NewKey]}, State) ->
  try edis_db:rename_nx(State#state.db, Key, NewKey) of
      ok -> tcp_number(1, State)
  catch
    _:already_exists ->
      tcp_number(0, State);
    _:not_found ->
      throw(no_such_key)
  end;
run(#edis_command{cmd = <<"TTL">>, args = [Key]}, State) ->
  try edis_db:ttl(State#state.db, Key) of
    undefined -> tcp_number(-1, State);
    Secs -> tcp_number(Secs, State)
  catch
    _:not_found ->
      tcp_number(-1, State)
  end;
run(#edis_command{cmd = <<"TYPE">>, args = [Key]}, State) ->
  try edis_db:type(State#state.db, Key) of
    Type -> tcp_ok(atom_to_binary(Type, utf8), State)
  catch
    _:not_found ->
      tcp_ok(<<"none">>, State)
  end;

%% -- Hashes ---------------------------------------------------------------------------------------
run(#edis_command{cmd = <<"HDEL">>, args = [Key | Fields]}, State) ->
  tcp_number(edis_db:hdel(State#state.db, Key, Fields), State);
run(#edis_command{cmd = <<"HEXISTS">>, args = [Key, Field]}, State) ->
  tcp_boolean(edis_db:hexists(State#state.db, Key, Field), State);
run(#edis_command{cmd = <<"HGET">>, args = [Key, Field]}, State) ->
  tcp_bulk(edis_db:hget(State#state.db, Key, Field), State);
run(#edis_command{cmd = <<"HGETALL">>, args = [Key]}, State) ->
  tcp_multi_bulk(lists:flatmap(fun tuple_to_list/1, edis_db:hget_all(State#state.db, Key)), State);
run(#edis_command{cmd = <<"HINCRBY">>, args = [Key, Field, Increment]}, State) ->
  try 
    tcp_number(edis_db:hincr(State#state.db, Key, Field, Increment), State)
  catch
    _:not_integer -> throw({not_integer, "hash value"})
  end;
run(#edis_command{cmd = <<"HKEYS">>, args = [Key]}, State) ->
  tcp_multi_bulk(edis_db:hkeys(State#state.db, Key), State);
run(#edis_command{cmd = <<"HLEN">>, args = [Key]}, State) ->
  tcp_number(edis_db:hlen(State#state.db, Key), State);
run(#edis_command{cmd = <<"HMGET">>, args = [Key | Fields]}, State) ->
  tcp_multi_bulk(edis_db:hget(State#state.db, Key, Fields), State);
run(#edis_command{cmd = <<"HMSET">>, args = [Key, FVs]}, State) ->
  _ = edis_db:hset(State#state.db, Key, FVs),
  tcp_ok(State);
run(#edis_command{cmd = <<"HSET">>, args = [Key, Field, Value]}, State) ->
  case edis_db:hset(State#state.db, Key, Field, Value) of
    inserted ->
      tcp_number(1, State);
    updated ->
      tcp_number(0, State)
  end;
run(#edis_command{cmd = <<"HSETNX">>, args = [Key, Field, Value]}, State) ->
  try edis_db:hset_nx(State#state.db, Key, Field, Value) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run(#edis_command{cmd = <<"HVALS">>, args = [Key]}, State) ->
  tcp_multi_bulk(edis_db:hvals(State#state.db, Key), State);
%% -- Lists ----------------------------------------------------------------------------------------
run(#edis_command{cmd = <<"BRPOP">>, args = Args}, State) ->
  [Timeout | Keys] = lists:reverse(Args),
  try
    {Key, Value} = edis_db:brpop(State#state.db, lists:reverse(Keys), Timeout),
    tcp_multi_bulk([Key, Value], State)
  catch
    _:timeout ->
      tcp_bulk(undefined, State);
    _:not_integer ->
      throw({not_integer, "timeout"})
  end;
run(#edis_command{cmd = <<"BLPOP">>, args = Args}, State) ->
  [Timeout | Keys] = lists:reverse(Args),
  try
    {Key, Value} = edis_db:blpop(State#state.db, lists:reverse(Keys), Timeout),
    tcp_multi_bulk([Key, Value], State)
  catch
    _:timeout ->
      tcp_bulk(undefined, State);
    _:not_integer ->
      throw({not_integer, "timeout"})
  end;
run(#edis_command{cmd = <<"BRPOPLPUSH">>, args = [Source, Destination, Timeout]}, State) ->
  try tcp_bulk(edis_db:brpop_lpush(State#state.db, Source, Destination, Timeout), State)
  catch
    _:timeout ->
      tcp_bulk(undefined, State);
    _:not_integer ->
      throw({not_integer, "timeout"})
  end;
run(#edis_command{cmd = <<"LINDEX">>, args = [Key, Index]}, State) ->
  tcp_bulk(edis_db:lindex(State#state.db, Key, Index), State);
run(#edis_command{cmd = <<"LINSERT">>, args = [Key, Position, Pivot, Value]}, State) ->
  try tcp_number(edis_db:linsert(State#state.db, Key, Position, Pivot, Value), State)
  catch
    _:not_found ->
      tcp_number(-1, State)
  end;
run(#edis_command{cmd = <<"LLEN">>, args = [Key]}, State) ->
  tcp_number(edis_db:llen(State#state.db, Key), State);
run(#edis_command{cmd = <<"LPOP">>, args = [Key]}, State) ->
  try edis_db:lpop(State#state.db, Key) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run(#edis_command{cmd = <<"LPUSH">>, args = [Key, Value]}, State) ->
  tcp_number(edis_db:lpush(State#state.db, Key, Value), State);
run(#edis_command{cmd = <<"LPUSHX">>, args = [Key, Value]}, State) ->
  try tcp_number(edis_db:lpush_x(State#state.db, Key, Value), State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run(#edis_command{cmd = <<"LRANGE">>, args = [Key, Start, Stop]}, State) ->
  tcp_multi_bulk(edis_db:lrange(State#state.db, Key, Start, Stop), State);
run(#edis_command{cmd = <<"LREM">>, args = [Key, Count, Value]}, State) ->
  try tcp_number(edis_db:lrem(State#state.db, Key, Count, Value), State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run(#edis_command{cmd = <<"LSET">>, args = [Key, Index, Value]}, State) ->
  try edis_db:lset(State#state.db, Key, Index, Value) of
    ok ->
      tcp_ok(State)
  catch
    _:not_found ->
      throw(no_such_key);
    _:out_of_range ->
      throw({out_of_range, "index"})
  end;
run(#edis_command{cmd = <<"LTRIM">>, args = [Key, Start, Stop]}, State) ->
  try edis_db:ltrim(State#state.db, Key, Start, Stop) of
    ok ->
      tcp_ok(State)
  catch
    _:not_found ->
      tcp_ok(State)
  end;
run(#edis_command{cmd = <<"RPOP">>, args = [Key]}, State) ->
  try edis_db:rpop(State#state.db, Key) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run(#edis_command{cmd = <<"RPOPLPUSH">>, args = [Source, Destination]}, State) ->
  try edis_db:rpop_lpush(State#state.db, Source, Destination) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run(#edis_command{cmd = <<"RPUSH">>, args = [Key, Value]}, State) ->
  tcp_number(edis_db:rpush(State#state.db, Key, Value), State);
run(#edis_command{cmd = <<"RPUSHX">>, args = [Key, Value]}, State) ->
  try edis_db:rpush_x(State#state.db, Key, Value) of
    NewLen -> tcp_number(NewLen, State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
%% -- Sets -----------------------------------------------------------------------------------------
run(#edis_command{cmd = <<"SADD">>, args = [Key, Members]}, State) ->
  tcp_number(edis_db:sadd(State#state.db, Key, Members), State);
run(#edis_command{cmd = <<"SCARD">>, args = [Key]}, State) ->
  tcp_number(edis_db:scard(State#state.db, Key), State);
run(#edis_command{cmd = <<"SDIFF">>, args = Keys}, State) ->
  tcp_multi_bulk(edis_db:sdiff(State#state.db, Keys), State);
run(#edis_command{cmd = <<"SDIFFSTORE">>, args = [Destination | Keys]}, State) ->
  tcp_number(edis_db:sdiff_store(State#state.db, Destination, Keys), State);
run(#edis_command{cmd = <<"SINTER">>, args = Keys}, State) ->
  tcp_multi_bulk(edis_db:sinter(State#state.db, Keys), State);
run(#edis_command{cmd = <<"SINTERSTORE">>, args = [Destination | Keys]}, State) ->
  tcp_number(edis_db:sinter_store(State#state.db, Destination, Keys), State);
run(#edis_command{cmd = <<"SISMEMBER">>, args = [Key, Member]}, State) ->
  tcp_boolean(edis_db:sismember(State#state.db, Key, Member), State);
run(#edis_command{cmd = <<"SMEMBERS">>, args = [Key]}, State) ->
  tcp_multi_bulk(edis_db:smembers(State#state.db, Key), State);
run(#edis_command{cmd = <<"SMOVE">>, args = [Source, Destination, Member]}, State) ->
  try tcp_boolean(edis_db:smove(State#state.db, Source, Destination, Member), State)
  catch
    _:not_found -> tcp_boolean(false, State)
  end;
run(#edis_command{cmd = <<"SPOP">>, args = [Key]}, State) ->
  try tcp_bulk(edis_db:spop(State#state.db, Key), State)
  catch
    _:not_found -> tcp_bulk(undefined, State)
  end;
run(#edis_command{cmd = <<"SRANDMEMBER">>, args = [Key]}, State) ->
  tcp_bulk(edis_db:srand_member(State#state.db, Key), State);
run(#edis_command{cmd = <<"SREM">>, args = [Key | Members]}, State) ->
  try tcp_number(edis_db:srem(State#state.db, Key, Members), State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run(#edis_command{cmd = <<"SUNION">>, args = Keys}, State) ->
  tcp_multi_bulk(edis_db:sunion(State#state.db, Keys), State);
run(#edis_command{cmd = <<"SUNIONSTORE">>, args = [Destination | Keys]}, State) ->
  tcp_number(edis_db:sunion_store(State#state.db, Destination, Keys), State);

%% -- Sets -----------------------------------------------------------------------------------------
run(#edis_command{cmd = <<"ZADD">>, args = [Key, SMs]}, State) ->
  tcp_number(edis_db:zadd(State#state.db, Key, SMs), State);
run(#edis_command{cmd = <<"ZCARD">>, args = [Key]}, State) ->
  tcp_number(edis_db:zcard(State#state.db, Key), State);
run(#edis_command{cmd = <<"ZCOUNT">>, args = [Key, Min, Max]}, State) ->
  try tcp_number(edis_db:zcount(State#state.db, Key, Min, Max), State)
  catch
    _:not_float ->
      throw({not_float, "min or max"})
  end;
run(#edis_command{cmd = <<"ZINCRBY">>, args = [Key, Increment, Member]}, State) ->
  tcp_float(edis_db:zincr(State#state.db, Key, Increment, Member), State);
run(#edis_command{cmd = <<"ZINTERSTORE">>, args = [Destination, WeightedKeys, Aggregate]}, State) ->
  tcp_number(
    edis_db:zinter_store(State#state.db, Destination, WeightedKeys, Aggregate),
    State);
run(#edis_command{cmd = <<"ZRANGE">>, args = [Key, Start, Stop]}, State) ->
  tcp_multi_bulk(
    [Member || {_Score, Member} <- edis_db:zrange(State#state.db, Key, Start, Stop)], State);
run(#edis_command{cmd = <<"ZRANGE">>, args = [Key, Start, Stop, with_scores]}, State) ->
  Reply =
      lists:flatten(
        [[Member, Score] || {Score, Member} <- edis_db:zrange(State#state.db, Key, Start, Stop)]),
  tcp_multi_bulk(Reply, State);
run(#edis_command{cmd = <<"ZRANGEBYSCORE">>, args = [Key, Min, Max, ShowScores, Limit]}, State) ->
  tcp_zrange(edis_db:zrange_by_score(State#state.db, Key, Min, Max), ShowScores, Limit, State);
run(#edis_command{cmd = <<"ZRANK">>, args = [Key, Member]}, State) ->
  tcp_number(edis_db:zrank(State#state.db, Key, Member), State);
run(#edis_command{cmd = <<"ZREM">>, args = [Key | Members]}, State) ->
  try tcp_number(edis_db:zrem(State#state.db, Key, Members), State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run(#edis_command{cmd = <<"ZREMRANGEBYRANK">>, args = [Key, Start, Stop]}, State) ->
  try tcp_number(edis_db:zrem_range_by_rank(State#state.db, Key, Start, Stop), State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run(#edis_command{cmd = <<"ZREMRANGEBYSCORE">>, args = [Key, Min, Max]}, State) ->
  try tcp_number(edis_db:zrem_range_by_score(State#state.db, Key, Min, Max), State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run(#edis_command{cmd = <<"ZREVRANGE">>, args = [Key, Start, Stop]}, State) ->
  tcp_multi_bulk(
    [Member || {_Score, Member} <- edis_db:zrev_range(State#state.db, Key, Start, Stop)], State);
run(#edis_command{cmd = <<"ZREVRANGE">>, args = [Key, Start, Stop, with_scores]}, State) ->
  tcp_multi_bulk(
    lists:flatten(
      [[Member, Score] ||
       {Score, Member} <- edis_db:zrev_range(State#state.db, Key, Start, Stop)]), State);
run(#edis_command{cmd = <<"ZREVRANGEBYSCORE">>, args = [Key, Min, Max, ShowScores, Limit]}, State) ->
  tcp_zrange(edis_db:zrev_range_by_score(State#state.db, Key, Min, Max), ShowScores, Limit, State);
run(#edis_command{cmd = <<"ZREVRANK">>, args = [Key, Member]}, State) ->
  tcp_number(edis_db:zrev_rank(State#state.db, Key, Member), State);
run(#edis_command{cmd = <<"ZSCORE">>, args = [Key, Member]}, State) ->
  tcp_float(edis_db:zscore(State#state.db, Key, Member), State);
run(#edis_command{cmd = <<"ZUNIONSTORE">>, args = [Destination, WeightedKeys, Aggregate]}, State) ->
  tcp_number(edis_db:zunion_store(State#state.db, Destination, WeightedKeys, Aggregate), State);
%% -- Server ---------------------------------------------------------------------------------------
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
run(#edis_command{cmd = <<"DBSIZE">>}, State) ->
  tcp_number(edis_db:size(State#state.db), State);
run(#edis_command{cmd = <<"FLUSHALL">>}, State) ->
  ok = edis_db:flush(),
  tcp_ok(State);
run(#edis_command{cmd = <<"FLUSHDB">>}, State) ->
  ok = edis_db:flush(State#state.db),
  tcp_ok(State);
run(#edis_command{cmd = <<"INFO">>}, State) ->
  Info = edis_db:info(State#state.db),
  tcp_bulk(lists:map(fun({K,V}) when is_binary(V) ->
                             io_lib:format("~p:~s~n", [K, V]);
                        ({K,V}) ->
                             io_lib:format("~p:~p~n", [K, V])
                     end, Info), State);
run(#edis_command{cmd = <<"LASTSAVE">>}, State) ->
  Ts = edis_db:last_save(State#state.db),
  tcp_number(erlang:round(Ts), State);
run(#edis_command{cmd = <<"MONITOR">>}, State) ->
  ok = edis_db_monitor:add_sup_handler(),
  tcp_ok(State);
run(#edis_command{cmd = <<"SAVE">>}, State) ->
  ok = edis_db:save(State#state.db),
  tcp_ok(State);
run(#edis_command{cmd = <<"SHUTDOWN">>}, State) ->
  _ = spawn(edis, stop, []),
  {stop, normal, State}.

%% @private
-spec tcp_boolean(boolean(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_boolean(true, State) -> tcp_number(1, State);
tcp_boolean(false, State) -> tcp_number(0, State).

%% @private
-spec tcp_multi_bulk([binary()], state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_multi_bulk(Lines, State) ->
  lists:foldl(
    fun(Float, {noreply, AccState}) when is_float(Float) ->
            tcp_float(Float, AccState);
       (Line, {noreply, AccState}) ->
            tcp_bulk(Line, AccState);
       (_Line, Error) ->
            Error
    end, tcp_send(["*", integer_to_list(erlang:length(Lines))], State), Lines).

%% @private
-spec tcp_bulk(undefined | iodata(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_bulk(undefined, State) ->
  tcp_send("$-1", State);
tcp_bulk(<<>>, State) ->
  tcp_send("$0\r\n", State);
tcp_bulk(Message, State) ->
  case tcp_send(["$", integer_to_list(iolist_size(Message))], State) of
    {noreply, NewState} -> tcp_send(Message, NewState);
    Error -> Error
  end.

%% @private
-spec tcp_number(undefined | integer(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_number(undefined, State) ->
  tcp_bulk(undefined, State);
tcp_number(Number, State) ->
  tcp_send([":", integer_to_list(Number)], State).

%% @private
-spec tcp_float(undefined | float(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_float(undefined, State) ->
  tcp_bulk(undefined, State);
tcp_float(Float, State) ->
  case erlang:trunc(Float) * 1.0 of
    Float -> tcp_bulk(integer_to_list(erlang:trunc(Float)), State);
    _ -> tcp_bulk(io_lib:format("~.18f", [Float]), State)
  end.

%% @private
-spec tcp_err(binary(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_err(Message, State) ->
  tcp_send(["-ERR ", Message], State).

%% @private
-spec tcp_ok(state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_ok(State) ->
  tcp_ok("OK", State).
%% @private
-spec tcp_ok(binary(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_ok(Message, State) ->
  tcp_send(["+", Message], State).


%% @private
-spec tcp_send(iodata(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_send(Message, State) ->
  ?CDEBUG(data, "~p << ~s~n", [State#state.peerport, Message]),
  try gen_tcp:send(State#state.socket, [Message, "\r\n"]) of
    ok ->
      {noreply, State};
    {error, closed} ->
      ?DEBUG("Connection closed~n", []),
      {stop, normal, State};
    {error, Error} ->
      ?THROW("Couldn't send msg through TCP~n\tError: ~p~n", [Error]),
      {stop, {error, Error}, State}
  catch
    _:{Exception, _} ->
      ?THROW("Couldn't send msg through TCP~n\tError: ~p~n", [Exception]),
      {stop, normal, State};
    _:Exception ->
      ?THROW("Couldn't send msg through TCP~n\tError: ~p~n", [Exception]),
      {stop, normal, State}
  end.

parse_float_limit(Bin) ->
  do_parse_float_limit(edis_util:lower(Bin)).

do_parse_float_limit(<<"-inf">>) -> neg_infinity;
do_parse_float_limit(<<"inf">>) -> infinity;
do_parse_float_limit(<<"+inf">>) -> infinity;
do_parse_float_limit(<<"-infinity">>) -> neg_infinity;
do_parse_float_limit(<<"infinity">>) -> infinity;
do_parse_float_limit(<<"+infinity">>) -> infinity;
do_parse_float_limit(<<$(, Rest/binary>>) -> {exc, edis_util:binary_to_float(Rest)};
do_parse_float_limit(Bin) -> {inc, edis_util:binary_to_float(Bin)}.

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
                 throw({not_float, "weight"})
             end, sum};
          {NK, R2L} when R2L == NK + 1 ->
            throw(syntax);
          {NK, R2L} when R2L == NK + 2 ->
            {try lists:map(fun edis_util:binary_to_float/1, lists:sublist(Rest2, 1, NK))
             catch
               _:not_float ->
                 throw({not_float, "weight"})
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
  C#edis_command{args = [Destination, lists:zip(Keys, Weights), Aggregate]}.

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
  C#edis_command{args = [Key, parse_float_limit(Min), parse_float_limit(Max), ShowScores, Limit]}.

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