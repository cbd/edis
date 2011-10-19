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
                peerport                :: pos_integer(),
                authenticated = false   :: boolean()}).
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
  try run_command(Cmd, Args, State)
  catch
    _:no_such_key ->
      ?ERROR("No such key for ~s on db #~p~n", [Cmd, State#state.db]),
      tcp_err("no such key", State);
    _:syntax ->
      ?ERROR("Syntax error for ~s on db #~p~n", [Cmd, State#state.db]),
      tcp_err("syntax error", State);
    _:not_integer ->
      ?ERROR("The value affected by ~s was not a integer on ~p~n", [Cmd, State#state.db]),
      tcp_err("value is not an integer or out of range", State);
    _:{not_integer, Field} ->
      ?ERROR("The value affected by ~s's ~s was not a integer on ~p~n", [Cmd, Field, State#state.db]),
      tcp_err([Field, " is not an integer or out of range"], State);
    _:{not_float, Field} ->
      ?ERROR("The value affected by ~s's ~s was not a float on ~p~n", [Cmd, Field, State#state.db]),
      tcp_err([Field, " is not a double"], State);
    _:{out_of_range, Field} ->
      ?ERROR("The value affected by ~s's ~s was out of range on ~p~n", [Cmd, Field, State#state.db]),
      tcp_err([Field, " is out of range"], State);
    _:{is_negative, Field} ->
      ?ERROR("The value affected by ~s's ~s was negative on ~p~n", [Cmd, Field, State#state.db]),
      tcp_err([Field, " is negative"], State);
    _:not_float ->
      ?ERROR("The value affected by ~s was not a float on ~p~n", [Cmd, State#state.db]),
      tcp_err("value is not a double", State);
    _:bad_item_type ->
      ?ERROR("Bad type running ~s on db #~p~n", [Cmd, State#state.db]),
      tcp_err("Operation against a key holding the wrong kind of value", State);
    _:source_equals_destination ->
      tcp_err("source and destinantion objects are the same", State);
    _:bad_arg_num ->
      tcp_err(["wrong number of arguments for '", Cmd, "' command"], State);
    _:{bad_arg_num, SubCmd} ->
      tcp_err(["wrong number of arguments for ", SubCmd], State);
    _:unauthorized ->
      ?WARN("Unauthorized user trying to do a ~s on ~p~n", [Cmd, State#state.db]),
      tcp_err("operation not permitted", State);
    _:{error, Reason} ->
      ?ERROR("Error running ~s on db #~p: ~p~n", [Cmd, State#state.db, Reason]),
      tcp_err(Reason, State);
    _:Error ->
      ?ERROR("Error running ~s on ~p:~n\t~p~n", [Cmd, State#state.db, Error]),
      tcp_err(io_lib:format("~p", [Error]), State)
  end.

%% @hidden
-spec handle_info(term(), state()) -> {noreply, state(), hibernate}.
handle_info(#edis_command{db = 0} = Command, State) ->
  tcp_ok(io_lib:format("~p ~p ~p", [Command#edis_command.timestamp,
                                    Command#edis_command.cmd,
                                    Command#edis_command.args]), State);
handle_info(#edis_command{} = Command, State) ->
  tcp_ok(io_lib:format("~p (db ~p) ~p ~p", [Command#edis_command.timestamp,
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
-spec run_command(binary(), [binary()], state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
%% -- Connection -----------------------------------------------------------------------------------
run_command(<<"QUIT">>, [], State) ->
  case tcp_ok(State) of
    {noreply, NewState} ->
      {stop, normal, NewState};
    Error ->
      Error
  end;
run_command(<<"QUIT">>, _, _State) -> throw(bad_arg_num);
run_command(<<"AUTH">>, [Password], State) ->
  case edis_config:get(requirepass) of
    undefined ->
      tcp_ok(State);
    Password ->
      tcp_ok(State#state{authenticated = true});
    _ ->
      tcp_err(<<"invalid password">>, State#state{authenticated = false})
  end;
run_command(<<"AUTH">>, _, _State) -> throw(bad_arg_num);
run_command(_, _, #state{authenticated = false}) -> throw(unauthorized);
run_command(<<"SELECT">>, [Db], State) ->
  try {edis_util:binary_to_integer(Db, 0), edis_config:get(databases)} of
    {DbIndex, Dbs} when DbIndex < 0 orelse DbIndex >= Dbs -> throw({error, "invalid DB index"});
    {DbIndex, _} -> tcp_ok(State#state{db = edis_db:process(DbIndex)})
  catch
    error:not_integer ->
      ?WARN("Switching to db 0 because we received '~s' as the db index. This behaviour was copied from redis-server~n", [Db]),
      tcp_ok(State#state{db = edis_db:process(0)})
  end;
run_command(<<"SELECT">>, _, _State) -> throw(bad_arg_num);
run_command(<<"PING">>, [], State) ->
  pong = edis_db:ping(State#state.db),
  tcp_ok(<<"PONG">>, State);
run_command(<<"PING">>, _, _State) -> throw(bad_arg_num);
run_command(<<"ECHO">>, [Word], State) ->
  tcp_bulk(Word, State);
run_command(<<"ECHO">>, _, _State) -> throw(bad_arg_num);

%% -- Strings --------------------------------------------------------------------------------------
run_command(<<"APPEND">>, [Key, Value], State) ->
  tcp_number(edis_db:append(State#state.db, Key, Value), State);
run_command(<<"APPEND">>, _, _State) -> throw(bad_arg_num);
run_command(<<"DECR">>, [Key], State) ->
  tcp_number(edis_db:decr(State#state.db, Key, 1), State);
run_command(<<"DECR">>, _, _State) -> throw(bad_arg_num);
run_command(<<"DECRBY">>, [Key, Decrement], State) ->
  tcp_number(edis_db:decr(State#state.db, Key,
                          edis_util:binary_to_integer(Decrement)), State);
run_command(<<"DECRBY">>, _, _State) -> throw(bad_arg_num);
run_command(<<"GET">>, [Key], State) ->
  tcp_bulk(edis_db:get(State#state.db, Key), State);
run_command(<<"GET">>, _, _State) -> throw(bad_arg_num);
run_command(<<"GETBIT">>, [Key, Offset], State) ->
  try edis_util:binary_to_integer(Offset) of
    O when O >= 0 ->
      tcp_number(edis_db:get_bit(State#state.db, Key, O), State);
    _ ->
      throw({not_integer, "bit offset"})
  catch
    _:not_integer ->
      throw({not_integer, "bit offset"})
  end;
run_command(<<"GETBIT">>, _, _State) -> throw(bad_arg_num);
run_command(<<"GETRANGE">>, [Key, Start, End], State) ->
  tcp_bulk(edis_db:get_range(
             State#state.db, Key,
             edis_util:binary_to_integer(Start),
             edis_util:binary_to_integer(End)), State);
run_command(<<"GETRANGE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"GETSET">>, [Key, Value], State) ->
  tcp_bulk(edis_db:get_and_set(State#state.db, Key, Value), State);
run_command(<<"GETSET">>, _, _State) -> throw(bad_arg_num);
run_command(<<"INCR">>, [Key], State) ->
  tcp_number(edis_db:incr(State#state.db, Key, 1), State);
run_command(<<"INCR">>, _, _State) -> throw(bad_arg_num);
run_command(<<"INCRBY">>, [Key, Increment], State) ->
  tcp_number(edis_db:incr(State#state.db, Key,
                          edis_util:binary_to_integer(Increment)), State);
run_command(<<"INCRBY">>, _, _State) -> throw(bad_arg_num);
run_command(<<"MGET">>, [], _State) -> throw(bad_arg_num);
run_command(<<"MGET">>, Keys, State) ->
  tcp_multi_bulk(edis_db:get(State#state.db, Keys), State);
run_command(<<"MSET">>, KVs, State) when KVs =/= [], length(KVs) rem 2 =:= 0 ->
  ok = edis_db:set(State#state.db, edis_util:make_pairs(KVs)),
  tcp_ok(State);
run_command(<<"MSET">>, _, _State) -> throw(bad_arg_num);
run_command(<<"MSETNX">>, KVs, State) when KVs =/= [], length(KVs) rem 2 =:= 0 ->
  try edis_db:set_nx(State#state.db, edis_util:make_pairs(KVs)) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run_command(<<"MSETNX">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SET">>, [Key, Value], State) ->
  ok = edis_db:set(State#state.db, Key, Value),
  tcp_ok(State);
run_command(<<"SET">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SETBIT">>, [Key, Offset, Bit], State) ->
  try {edis_util:binary_to_integer(Offset), Bit} of
    {O, Bit} when O >= 0, Bit == <<"0">> ->
      tcp_number(edis_db:set_bit(State#state.db, Key, O, 0), State);
    {O, Bit} when O >= 0, Bit == <<"1">> ->
      tcp_number(edis_db:set_bit(State#state.db, Key, O, 1), State);
    {O, _BadBit} when O >= 0 ->
      throw({not_integer, "bit"});
    _ ->
      throw({not_integer, "bit offset"})
  catch
    _:not_integer ->
      throw({not_integer, "bit offset"})
  end;
run_command(<<"SETBIT">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SETEX">>, [Key, Seconds, Value], State) ->
  case edis_util:binary_to_integer(Seconds) of
    Secs when Secs =< 0 -> throw({error, "invalid expire time in SETEX"});
    Secs ->
      ok = edis_db:set_ex(State#state.db, Key, Secs, Value),
      tcp_ok(State)
  end;
run_command(<<"SETEX">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SETNX">>, [Key, Value], State) ->
  try edis_db:set_nx(State#state.db, Key, Value) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run_command(<<"SETNX">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SETRANGE">>, [Key, Offset, Value], State) ->
  case edis_util:binary_to_integer(Offset) of
    Off when Off < 0 -> throw({out_of_range, "offset"});
    Off ->
      tcp_number(edis_db:set_range(State#state.db, Key, Off, Value), State)
  end;
run_command(<<"SETRANGE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"STRLEN">>, [Key], State) ->
  tcp_number(edis_db:str_len(State#state.db, Key), State);
run_command(<<"STRLEN">>, _, _State) -> throw(bad_arg_num);

%% -- Keys -----------------------------------------------------------------------------------------
run_command(<<"DEL">>, [], _State) -> throw(bad_arg_num);
run_command(<<"DEL">>, Keys, State) ->
  tcp_number(edis_db:del(State#state.db, Keys), State);
run_command(<<"EXISTS">>, [Key], State) ->
  tcp_boolean(edis_db:exists(State#state.db, Key), State);
run_command(<<"EXISTS">>, _, _State) -> throw(bad_arg_num);
run_command(<<"EXPIRE">>, [Key, Seconds], State) ->
  tcp_boolean(edis_db:expire(State#state.db, Key, edis_util:binary_to_integer(Seconds)), State);
run_command(<<"EXPIRE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"EXPIREAT">>, [Key, Timestamp], State) ->
  tcp_boolean(edis_db:expire_at(State#state.db, Key, edis_util:binary_to_integer(Timestamp)), State);
run_command(<<"EXPIREAT">>, _, _State) -> throw(bad_arg_num);
run_command(<<"KEYS">>, [Pattern], State) ->
  tcp_multi_bulk(edis_db:keys(State#state.db, edis_util:glob_to_re(Pattern)), State);
run_command(<<"KEYS">>, _, _State) -> throw(bad_arg_num);
run_command(<<"MOVE">>, [Key, Db], State) ->
  DbIndex = edis_util:binary_to_integer(Db, 0),
  case {DbIndex, edis_config:get(databases), State#state.db, edis_db:process(DbIndex)} of
    {DbIndex, Dbs, _, _} when DbIndex < 0 orelse DbIndex > Dbs ->
      throw({out_of_range, "index"});
    {_, _, CurrentDb, CurrentDb} ->
      throw(source_equals_destination);
    {_, _, CurrentDb, DestDb} ->
      tcp_boolean(edis_db:move(CurrentDb, Key, DestDb), State)
  end;
run_command(<<"MOVE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"OBJECT">>, [SubCommand | Rest], State) ->
  run_command(<<"OBJECT ", (edis_util:upper(SubCommand))/binary>>, Rest, State);
run_command(<<"OBJECT REFCOUNT">>, [Key], State) ->
  %%XXX: Not *really* implemented
  case edis_db:exists(State#state.db, Key) of
    true -> tcp_number(1, State);
    false -> tcp_bulk(undefined, State)
  end;
run_command(<<"OBJECT ENCODING">>, [Key], State) ->
  Reply =
      case edis_db:encoding(State#state.db, Key) of
        undefined -> undefined;
        Encoding -> atom_to_binary(Encoding, utf8)
      end,
  tcp_bulk(Reply, State);
run_command(<<"OBJECT IDLETIME">>, [Key], State) ->
  tcp_number(edis_db:idle_time(State#state.db, Key), State);
run_command(<<"OBJECT", _Rest/binary>>, _, _State) ->
  throw({error, "Syntax error. Try OBJECT (refcount|encoding|idletime)"});
run_command(<<"PERSIST">>, [Key], State) ->
  tcp_boolean(edis_db:persist(State#state.db, Key), State);
run_command(<<"PERSIST">>, _, _State) -> throw(bad_arg_num);
run_command(<<"RANDOMKEY">>, [], State) ->
  tcp_bulk(edis_db:random_key(State#state.db), State);
run_command(<<"RANDOMKEY">>, _, _State) -> throw(bad_arg_num);
run_command(<<"RENAME">>, [Key, Key], _State) ->
  throw({error, "source and destination objects are the same"});
run_command(<<"RENAME">>, [Key, NewKey], State) ->
  try edis_db:rename(State#state.db, Key, NewKey) of
    ok -> tcp_ok(State)
  catch
    _:not_found ->
      throw(no_such_key)
  end;
run_command(<<"RENAME">>, _, _State) -> throw(bad_arg_num);
run_command(<<"RENAMENX">>, [Key, Key], _State) ->
  throw(source_equals_destination);
run_command(<<"RENAMENX">>, [Key, NewKey], State) ->
  try edis_db:rename_nx(State#state.db, Key, NewKey) of
      ok -> tcp_number(1, State)
  catch
    _:already_exists ->
      tcp_number(0, State);
    _:not_found ->
      throw(no_such_key)
  end;
run_command(<<"RENAMENX">>, _, _State) -> throw(bad_arg_num);
run_command(<<"TTL">>, [Key], State) ->
  try edis_db:ttl(State#state.db, Key) of
    undefined -> tcp_number(-1, State);
    Secs -> tcp_number(Secs, State)
  catch
    _:not_found ->
      tcp_number(-1, State)
  end;
run_command(<<"TTL">>, _, _State) -> throw(bad_arg_num);
run_command(<<"TYPE">>, [Key], State) ->
  try edis_db:type(State#state.db, Key) of
    Type -> tcp_ok(atom_to_binary(Type, utf8), State)
  catch
    _:not_found ->
      tcp_ok(<<"none">>, State)
  end;
run_command(<<"TYPE">>, _, _State) -> throw(bad_arg_num);

%% -- Hashes ---------------------------------------------------------------------------------------
run_command(<<"HDEL">>, [], _State) -> throw(bad_arg_num);
run_command(<<"HDEL">>, [_Key], _State) -> throw(bad_arg_num);
run_command(<<"HDEL">>, [Key | Fields], State) ->
  tcp_number(edis_db:hdel(State#state.db, Key, Fields), State);
run_command(<<"HEXISTS">>, [Key, Field], State) ->
  tcp_boolean(edis_db:hexists(State#state.db, Key, Field), State);
run_command(<<"HEXISTS">>, _, _State) -> throw(bad_arg_num);
run_command(<<"HGET">>, [Key, Field], State) ->
  tcp_bulk(edis_db:hget(State#state.db, Key, Field), State);
run_command(<<"HGET">>, _, _State) -> throw(bad_arg_num);
run_command(<<"HGETALL">>, [Key], State) ->
  tcp_multi_bulk(lists:flatmap(fun tuple_to_list/1, edis_db:hget_all(State#state.db, Key)), State);
run_command(<<"HGETALL">>, _, _State) -> throw(bad_arg_num);
run_command(<<"HINCRBY">>, [Key, Field, Increment], State) ->
  Inc = edis_util:binary_to_integer(Increment),
  try 
    tcp_number(edis_db:hincr(State#state.db, Key, Field, Inc), State)
  catch
    _:not_integer -> throw({not_integer, "hash value"})
  end;
run_command(<<"HINCRBY">>, _, _State) -> throw(bad_arg_num);
run_command(<<"HKEYS">>, [Key], State) ->
  tcp_multi_bulk(edis_db:hkeys(State#state.db, Key), State);
run_command(<<"HKEYS">>, _, _State) -> throw(bad_arg_num);
run_command(<<"HLEN">>, [Key], State) ->
  tcp_number(edis_db:hlen(State#state.db, Key), State);
run_command(<<"HLEN">>, _, _State) -> throw(bad_arg_num);
run_command(<<"HMGET">>, [Key | Fields], State) when Fields =/= [] ->
  tcp_multi_bulk(edis_db:hget(State#state.db, Key, Fields), State);
run_command(<<"HMGET">>, _, _State) -> throw({bad_arg_num, "HMGET"});
run_command(<<"HMSET">>, [Key | FVs], State) when FVs =/= [], length(FVs) rem 2 =:= 0 ->
  _ = edis_db:hset(State#state.db, Key, edis_util:make_pairs(FVs)),
  tcp_ok(State);
run_command(<<"HMSET">>, _, _State) -> throw({bad_arg_num, "HMSET"});
run_command(<<"HSET">>, [Key, Field, Value], State) ->
  case edis_db:hset(State#state.db, Key, Field, Value) of
    inserted ->
      tcp_number(1, State);
    updated ->
      tcp_number(0, State)
  end;
run_command(<<"HSET">>, _, _State) -> throw(bad_arg_num);
run_command(<<"HSETNX">>, [Key, Field, Value], State) ->
  try edis_db:hset_nx(State#state.db, Key, Field, Value) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run_command(<<"HSETNX">>, _, _State) -> throw(bad_arg_num);
run_command(<<"HVALS">>, [Key], State) ->
  tcp_multi_bulk(edis_db:hvals(State#state.db, Key), State);
run_command(<<"HVALS">>, _, _State) -> throw(bad_arg_num);

%% -- Lists ----------------------------------------------------------------------------------------
run_command(<<"BRPOP">>, [], _State) -> throw(bad_arg_num);
run_command(<<"BRPOP">>, [_], _State) -> throw(bad_arg_num);
run_command(<<"BRPOP">>, Args, State) ->
  [Timeout | Keys] = lists:reverse(Args),
  try
    case edis_util:binary_to_integer(Timeout) of
      T when T < 0 ->
        throw({is_negative, "timeout"});
      0 ->
        {Key, Value} = edis_db:brpop(State#state.db, lists:reverse(Keys), infinity),
        tcp_multi_bulk([Key, Value], State);
      T ->
        {Key, Value} = edis_db:brpop(State#state.db, lists:reverse(Keys), T),
        tcp_multi_bulk([Key, Value], State)
    end
  catch
    _:timeout ->
      tcp_bulk(undefined, State);
    _:not_integer ->
      throw({not_integer, "timeout"})
  end;
run_command(<<"BLPOP">>, [], _State) -> throw(bad_arg_num);
run_command(<<"BLPOP">>, [_], _State) -> throw(bad_arg_num);
run_command(<<"BLPOP">>, Args, State) ->
  [Timeout | Keys] = lists:reverse(Args),
  try
    case edis_util:binary_to_integer(Timeout) of
      T when T < 0 ->
        throw({is_negative, "timeout"});
      0 ->
        {Key, Value} = edis_db:blpop(State#state.db, lists:reverse(Keys), infinity),
        tcp_multi_bulk([Key, Value], State);
      T ->
        {Key, Value} = edis_db:blpop(State#state.db, lists:reverse(Keys), T),
        tcp_multi_bulk([Key, Value], State)
    end
  catch
    _:timeout ->
      tcp_bulk(undefined, State);
    _:not_integer ->
      throw({not_integer, "timeout"})
  end;
run_command(<<"BRPOPLPUSH">>, [Source, Destination, Timeout], State) ->
  try
    case edis_util:binary_to_integer(Timeout) of
      T when T < 0 ->
        throw({is_negative, "timeout"});
      0 ->
        tcp_bulk(edis_db:brpop_lpush(State#state.db, Source, Destination, infinity), State);
      T ->
        tcp_bulk(edis_db:brpop_lpush(State#state.db, Source, Destination, T), State)
    end
  catch
    _:timeout ->
      tcp_bulk(undefined, State);
    _:not_integer ->
      throw({not_integer, "timeout"})
  end;
run_command(<<"BRPOPLPUSH">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LINDEX">>, [Key, Index], State) ->
  tcp_bulk(edis_db:lindex(State#state.db, Key, edis_util:binary_to_integer(Index, 0)), State);
run_command(<<"LINDEX">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LINSERT">>, [Key, Position, Pivot, Value], State) ->
  try
    case edis_util:upper(Position) of
      <<"BEFORE">> ->
        tcp_number(edis_db:linsert(State#state.db, Key, before, Pivot, Value), State);
      <<"AFTER">> ->
        tcp_number(edis_db:linsert(State#state.db, Key, 'after', Pivot, Value), State);
      _ ->
        throw(syntax)
    end
  catch
    _:not_found ->
      tcp_number(-1, State)
  end;
run_command(<<"LINSERT">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LLEN">>, [Key], State) ->
  tcp_number(edis_db:llen(State#state.db, Key), State);
run_command(<<"LLEN">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LPOP">>, [Key], State) ->
  try edis_db:lpop(State#state.db, Key) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run_command(<<"LPOP">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LPUSH">>, [Key, Value], State) ->
  tcp_number(edis_db:lpush(State#state.db, Key, Value), State);
run_command(<<"LPUSH">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LPUSHX">>, [Key, Value], State) ->
  try edis_db:lpush_x(State#state.db, Key, Value) of
    NewLen -> tcp_number(NewLen, State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run_command(<<"LPUSHX">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LRANGE">>, [Key, Start, Stop], State) ->
  tcp_multi_bulk(edis_db:lrange(State#state.db, Key,
                                edis_util:binary_to_integer(Start, 0),
                                edis_util:binary_to_integer(Stop, 0)), State);
run_command(<<"LRANGE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LREM">>, [Key, Count, Value], State) ->
  try edis_db:lrem(State#state.db, Key, edis_util:binary_to_integer(Count, 0), Value) of
    Removed ->
      tcp_number(Removed, State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run_command(<<"LREM">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LSET">>, [Key, Index, Value], State) ->
  try edis_db:lset(State#state.db, Key, edis_util:binary_to_integer(Index, 0), Value) of
    ok ->
      tcp_ok(State)
  catch
    _:not_found ->
      throw(no_such_key);
    _:out_of_range ->
      throw({out_of_range, "index"})
  end;
run_command(<<"LSET">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LTRIM">>, [Key, Start, Stop], State) ->
  try edis_db:ltrim(State#state.db, Key,
                     edis_util:binary_to_integer(Start),
                     edis_util:binary_to_integer(Stop)) of
    ok ->
      tcp_ok(State)
  catch
    _:not_found ->
      tcp_ok(State)
  end;
run_command(<<"LTRIM">>, _, _State) -> throw(bad_arg_num);
run_command(<<"RPOP">>, [Key], State) ->
  try edis_db:rpop(State#state.db, Key) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run_command(<<"RPOP">>, _, _State) -> throw(bad_arg_num);
run_command(<<"RPOPLPUSH">>, [Source, Destination], State) ->
  try edis_db:rpop_lpush(State#state.db, Source, Destination) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run_command(<<"RPOPLPUSH">>, _, _State) -> throw(bad_arg_num);
run_command(<<"RPUSH">>, [Key, Value], State) ->
  tcp_number(edis_db:rpush(State#state.db, Key, Value), State);
run_command(<<"RPUSH">>, _, _State) -> throw(bad_arg_num);
run_command(<<"RPUSHX">>, [Key, Value], State) ->
  try edis_db:rpush_x(State#state.db, Key, Value) of
    NewLen -> tcp_number(NewLen, State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run_command(<<"RPUSHX">>, _, _State) -> throw(bad_arg_num);

%% -- Sets -----------------------------------------------------------------------------------------
run_command(<<"SADD">>, [Key, Member | Members], State) ->
  tcp_number(edis_db:sadd(State#state.db, Key, [Member | Members]), State);
run_command(<<"SADD">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SCARD">>, [Key], State) ->
  tcp_number(edis_db:scard(State#state.db, Key), State);
run_command(<<"SCARD">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SDIFF">>, [Key|Keys], State) ->
  tcp_multi_bulk(edis_db:sdiff(State#state.db, [Key|Keys]), State);
run_command(<<"SDIFF">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SDIFFSTORE">>, [Destination, Key | Keys], State) ->
  tcp_number(edis_db:sdiff_store(State#state.db, Destination, [Key|Keys]), State);
run_command(<<"SDIFFSTORE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SINTER">>, [Key|Keys], State) ->
  tcp_multi_bulk(edis_db:sinter(State#state.db, [Key|Keys]), State);
run_command(<<"SINTER">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SINTERSTORE">>, [Destination, Key | Keys], State) ->
  tcp_number(edis_db:sinter_store(State#state.db, Destination, [Key|Keys]), State);
run_command(<<"SINTERSTORE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SISMEMBER">>, [Key, Member], State) ->
  tcp_boolean(edis_db:sismember(State#state.db, Key, Member), State);
run_command(<<"SISMEMBER">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SMEMBERS">>, [Key], State) ->
  tcp_multi_bulk(edis_db:smembers(State#state.db, Key), State);
run_command(<<"SMEMBERS">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SMOVE">>, [Source, Destination, Member], State) ->
  try edis_db:smove(State#state.db, Source, Destination, Member) of
    Moved -> tcp_boolean(Moved, State)
  catch
    _:not_found -> tcp_boolean(false, State)
  end;
run_command(<<"SMOVE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SPOP">>, [Key], State) ->
  try edis_db:spop(State#state.db, Key) of
    Member -> tcp_bulk(Member, State)
  catch
    _:not_found -> tcp_bulk(undefined, State)
  end;
run_command(<<"SPOP">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SRANDMEMBER">>, [Key], State) ->
  tcp_bulk(edis_db:srand_member(State#state.db, Key), State);
run_command(<<"SRANDMEMBER">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SREM">>, [Key, Member | Members], State) ->
  try edis_db:srem(State#state.db, Key, [Member | Members]) of
    Count -> tcp_number(Count, State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run_command(<<"SREM">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SUNION">>, [Key|Keys], State) ->
  tcp_multi_bulk(edis_db:sunion(State#state.db, [Key|Keys]), State);
run_command(<<"SUNION">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SUNIONSTORE">>, [Destination, Key | Keys], State) ->
  tcp_number(edis_db:sunion_store(State#state.db, Destination, [Key|Keys]), State);
run_command(<<"SUNIONSTORE">>, _, _State) -> throw(bad_arg_num);

%% -- Sets -----------------------------------------------------------------------------------------
run_command(<<"ZADD">>, [Key | SMs], State) when SMs =/= [], length(SMs) rem 2 =:= 0 ->
  ParsedSMs = [{edis_util:binary_to_float(S), M} || {S, M} <- edis_util:make_pairs(SMs)],
  tcp_number(edis_db:zadd(State#state.db, Key, ParsedSMs), State);
run_command(<<"ZADD">>, _, _State) -> throw(bad_arg_num);
run_command(<<"ZCARD">>, [Key], State) ->
  tcp_number(edis_db:zcard(State#state.db, Key), State);
run_command(<<"ZCARD">>, _, _State) -> throw(bad_arg_num);
run_command(<<"ZCOUNT">>, [Key, Min, Max], State) ->
  try tcp_number(edis_db:zcount(State#state.db, Key,
                                parse_float_limit(Min), parse_float_limit(Max)), State)
  catch
    _:not_float ->
      throw({not_float, "min or max"})
  end;
run_command(<<"ZCOUNT">>, _, _State) -> throw(bad_arg_num);
run_command(<<"ZINCRBY">>, [Key, Increment, Member], State) ->
  tcp_float(edis_db:zincr(State#state.db, Key, edis_util:binary_to_float(Increment), Member), State);
run_command(<<"ZINCRBY">>, _, _State) -> throw(bad_arg_num);
run_command(<<"ZINTERSTORE">>, [Destination, NumKeys | Rest], State) ->
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
  tcp_number(
    edis_db:zinter_store(State#state.db, Destination, lists:zip(Keys, Weights), Aggregate),
    State);
run_command(<<"ZINTERSTORE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"ZRANGE">>, [Key, Start, Stop], State) ->
  Reply =
    [Member ||
     {_Score, Member} <- edis_db:zrange(State#state.db, Key,
                                        edis_util:binary_to_integer(Start, 0),
                                        edis_util:binary_to_integer(Stop, 0))],
  tcp_multi_bulk(Reply, State);
run_command(<<"ZRANGE">>, [Key, Start, Stop, Option], State) ->
  case edis_util:upper(Option) of
    <<"WITHSCORES">> ->
      Reply =
        lists:flatten(
          [[Member, Score] ||
           {Score, Member} <- edis_db:zrange(State#state.db, Key,
                                             edis_util:binary_to_integer(Start, 0),
                                             edis_util:binary_to_integer(Stop, 0))]),
      tcp_multi_bulk(Reply, State);
    _ ->
      throw(syntax)
  end;
run_command(<<"ZRANGE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"ZRANGEBYSCORE">>, [Key, Min, Max | Options], State) when 0 =< length(Options),
                                                                        length(Options) =< 4->
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
  
  Range =
    try edis_db:zrange_by_score(State#state.db, Key, parse_float_limit(Min), parse_float_limit(Max))
    catch
      _:not_float ->
        throw({not_float, "min or max"})
    end,
  
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
  tcp_multi_bulk(Reply, State);
run_command(<<"ZRANGEBYSCORE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"ZRANK">>, [Key, Member], State) ->
  tcp_number(edis_db:zrank(State#state.db, Key, Member), State);
run_command(<<"ZRANK">>, _, _State) -> throw(bad_arg_num);


%% -- Server ---------------------------------------------------------------------------------------
run_command(<<"CONFIG">>, [SubCommand | Rest], State) ->
  run_command(<<"CONFIG ", (edis_util:upper(SubCommand))/binary>>, Rest, State);
run_command(<<"CONFIG GET">>, [Pattern], State) ->
  Configs = edis_config:get(edis_util:glob_to_re(Pattern)),
  Lines = lists:flatten(
            [[atom_to_binary(K, utf8),
              case V of
                undefined -> undefined;
                V when is_binary(V) -> V;
                V -> erlang:iolist_to_binary(io_lib:format("~p", [V]))
              end] || {K, V} <- Configs]),
  tcp_multi_bulk(Lines, State);
run_command(<<"CONFIG">>, _, _State) -> throw(bad_arg_num);
run_command(<<"CONFIG GET">>, _, _State) -> throw({bad_arg_num, "CONFIG GET"});
run_command(<<"CONFIG SET">>, [Key | Values], State) ->
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
  try edis_config:set(Param, Value) of
    ok -> tcp_ok(State)
  catch
    _:invalid_param ->
      throw({error, ["Invalid argument '", Value, "' for CONFIG SET '", Key, "'"]});
    _:unsupported_param ->
      throw({error, ["Unsopported CONFIG parameter: ", Key]})
  end;
run_command(<<"CONFIG SET">>, _, _State) -> throw({bad_arg_num, "CONFIG SET"});
run_command(<<"CONFIG RESETSTAT">>, [], State) ->
  %%TODO: Reset the statistics
  tcp_ok(State);
run_command(<<"CONFIG RESETSTAT">>, _, _State) -> throw({bad_arg_num, "CONFIG RESETSTAT"});
run_command(<<"DBSIZE">>, [], State) ->
  tcp_number(edis_db:size(State#state.db), State);
run_command(<<"DBSIZE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"FLUSHALL">>, [], State) ->
  ok = edis_db:flush(),
  tcp_ok(State);
run_command(<<"FLUSHALL">>, _, _State) -> throw(bad_arg_num);
run_command(<<"FLUSHDB">>, [], State) ->
  ok = edis_db:flush(State#state.db),
  tcp_ok(State);
run_command(<<"FLUSHDB">>, _, _State) -> throw(bad_arg_num);
run_command(<<"INFO">>, [], State) ->
  Info = edis_db:info(State#state.db),
  tcp_bulk(lists:map(fun({K,V}) when is_binary(V) ->
                             io_lib:format("~p:~s~n", [K, V]);
                        ({K,V}) ->
                             io_lib:format("~p:~p~n", [K, V])
                     end, Info), State);
run_command(<<"INFO">>, _, _State) -> throw(bad_arg_num);
run_command(<<"LASTSAVE">>, [], State) ->
  Ts = edis_db:last_save(State#state.db),
  tcp_number(erlang:round(Ts), State);
run_command(<<"LASTSAVE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"MONITOR">>, [], State) ->
  ok = edis_db_monitor:add_sup_handler(),
  tcp_ok(State);
run_command(<<"MONITOR">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SAVE">>, [], State) ->
  ok = edis_db:save(State#state.db),
  tcp_ok(State);
run_command(<<"SAVE">>, _, _State) -> throw(bad_arg_num);
run_command(<<"SHUTDOWN">>, [], State) ->
  _ = spawn(edis, stop, []),
  {stop, normal, State};
run_command(<<"SHUTDOWN">>, _, _State) ->
  throw(bad_arg_num);
run_command(Command, Args, _State)
  when Command == <<"SYNC">> orelse Command == <<"SLOWLOG">>
  orelse Command == <<"SLAVEOF">> ->
  ?WARN("Unsupported command: ~s~p~n", [Command, Args]),
  throw({error, "unsupported command"});

%% -- Errors ---------------------------------------------------------------------------------------
run_command(Command, _Args, State) ->
  tcp_err(["unknown command '", Command, "'"], State).

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