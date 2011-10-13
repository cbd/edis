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
    _:not_integer ->
      ?ERROR("The value affected by ~s was not a integer on db #~p~n", [Cmd, State#state.db]),
      tcp_err("value is not an integer or out of range", State);
    _:bad_item_type ->
      ?ERROR("Bad type running ~s on db #~p~n", [Cmd, State#state.db]),
      tcp_err("Operation against a key holding the wrong kind of value", State);
    _:Error ->
      ?ERROR("Error running ~s on db #~p:~n\t~p~n", [Cmd, State#state.db, Error]),
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
run_command(<<"QUIT">>, _, State) ->
  tcp_err("wrong number of arguments for 'QUIT' command", State);
run_command(<<"AUTH">>, [Password], State) ->
  case edis_config:get(requirepass) of
    undefined ->
      tcp_ok(State);
    Password ->
      tcp_ok(State#state{authenticated = true});
    _ ->
      tcp_err(<<"invalid password">>, State#state{authenticated = false})
  end;
run_command(<<"AUTH">>, _, State) ->
  tcp_err("wrong number of arguments for 'AUTH' command", State);
run_command(_, _, State = #state{authenticated = false}) ->
  tcp_err("operation not permitted", State);
run_command(<<"SELECT">>, [Db], State) ->
  try {edis_util:binary_to_integer(Db), edis_config:get(databases)} of
    {DbIndex, Dbs} when DbIndex < 0 orelse DbIndex >= Dbs ->
      tcp_err("invalid DB index", State);
    {DbIndex, _} ->
      tcp_ok(State#state{db = edis_db:process(DbIndex)})
  catch
    error:not_integer ->
      ?WARN("Switching to db 0 because we received '~s' as the db index. This behaviour was copied from redis-server~n", [Db]),
      tcp_ok(State#state{db = edis_db:process(0)})
  end;
run_command(<<"SELECT">>, _, State) ->
  tcp_err("wrong number of arguments for 'SELECT' command", State);
run_command(<<"PING">>, [], State) ->
  pong = edis_db:ping(State#state.db),
  tcp_ok(<<"PONG">>, State);
run_command(<<"PING">>, _, State) ->
  tcp_err("wrong number of arguments for 'PING' command", State);
run_command(<<"ECHO">>, [Word], State) ->
  tcp_bulk(Word, State);
run_command(<<"ECHO">>, _, State) ->
  tcp_err("wrong number of arguments for 'ECHO' command", State);

%% -- Strings --------------------------------------------------------------------------------------
run_command(<<"APPEND">>, [Key, Value], State) ->
  tcp_number(edis_db:append(State#state.db, Key, Value), State);
run_command(<<"APPEND">>, _, State) ->
  tcp_err("wrong number of arguments for 'APPEND' command", State);
run_command(<<"DECR">>, [Key], State) ->
  tcp_number(edis_db:decr(State#state.db, Key, 1), State);
run_command(<<"DECR">>, _, State) ->
  tcp_err("wrong number of arguments for 'DECR' command", State);
run_command(<<"DECRBY">>, [Key, Decrement], State) ->
  tcp_number(edis_db:decr(State#state.db, Key,
                          edis_util:binary_to_integer(Decrement)), State);
run_command(<<"DECRBY">>, _, State) ->
  tcp_err("wrong number of arguments for 'DECRBY' command", State);
run_command(<<"GET">>, [Key], State) ->
  tcp_bulk(edis_db:get(State#state.db, Key), State);
run_command(<<"GET">>, _, State) ->
  tcp_err("wrong number of arguments for 'GET' command", State);
run_command(<<"GETBIT">>, [Key, Offset], State) ->
  try edis_util:binary_to_integer(Offset) of
    O when O >= 0 ->
      tcp_number(edis_db:get_bit(State#state.db, Key, O), State);
    _ ->
      tcp_err("bit offset is not an integer or out of range", State)
  catch
    _:not_integer ->
      tcp_err("bit offset is not an integer or out of range", State)
  end;
run_command(<<"GETBIT">>, _, State) ->
  tcp_err("wrong number of arguments for 'GETBIT' command", State);
run_command(<<"GETRANGE">>, [Key, Start, End], State) ->
  tcp_bulk(edis_db:get_range(
             State#state.db, Key,
             edis_util:binary_to_integer(Start),
             edis_util:binary_to_integer(End)), State);
run_command(<<"GETRANGE">>, _, State) ->
  tcp_err("wrong number of arguments for 'GETRANGE' command", State);
run_command(<<"GETSET">>, [Key, Value], State) ->
  tcp_bulk(edis_db:get_and_set(State#state.db, Key, Value), State);
run_command(<<"GETSET">>, _, State) ->
  tcp_err("wrong number of arguments for 'GETSET' command", State);
run_command(<<"INCR">>, [Key], State) ->
  tcp_number(edis_db:incr(State#state.db, Key, 1), State);
run_command(<<"INCR">>, _, State) ->
  tcp_err("wrong number of arguments for 'INCR' command", State);
run_command(<<"INCRBY">>, [Key, Increment], State) ->
  tcp_number(edis_db:incr(State#state.db, Key,
                          edis_util:binary_to_integer(Increment)), State);
run_command(<<"INCRBY">>, _, State) ->
  tcp_err("wrong number of arguments for 'INCRBY' command", State);
run_command(<<"MGET">>, [], State) ->
  tcp_err("wrong number of arguments for 'MGET' command", State);
run_command(<<"MGET">>, Keys, State) ->
  tcp_multi_bulk(edis_db:get(State#state.db, Keys), State);
run_command(<<"MSET">>, KVs, State) when KVs =/= [], length(KVs) rem 2 =:= 0 ->
  ok = edis_db:set(State#state.db, edis_util:make_pairs(KVs)),
  tcp_ok(State);
run_command(<<"MSET">>, _, State) ->
  tcp_err("wrong number of arguments for 'MSET' command", State);
run_command(<<"MSETNX">>, KVs, State) when KVs =/= [], length(KVs) rem 2 =:= 0 ->
  try edis_db:set_nx(State#state.db, edis_util:make_pairs(KVs)) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run_command(<<"MSETNX">>, _, State) ->
  tcp_err("wrong number of arguments for 'MSETNX' command", State);
run_command(<<"SET">>, [Key, Value], State) ->
  ok = edis_db:set(State#state.db, Key, Value),
  tcp_ok(State);
run_command(<<"SET">>, _, State) ->
  tcp_err("wrong number of arguments for 'SET' command", State);
run_command(<<"SETBIT">>, [Key, Offset, Bit], State) ->
  try {edis_util:binary_to_integer(Offset), Bit} of
    {O, Bit} when O >= 0, Bit == <<"0">> ->
      tcp_number(edis_db:set_bit(State#state.db, Key, O, 0), State);
    {O, Bit} when O >= 0, Bit == <<"1">> ->
      tcp_number(edis_db:set_bit(State#state.db, Key, O, 1), State);
    {O, _BadBit} when O >= 0 ->
      tcp_err("bit is not an integer or out of range", State);
    _ ->
      tcp_err("bit offset is not an integer or out of range", State)
  catch
    _:not_integer ->
      tcp_err("bit offset is not an integer or out of range", State)
  end;
run_command(<<"SETBIT">>, _, State) ->
  tcp_err("wrong number of arguments for 'SETBIT' command", State);
run_command(<<"SETEX">>, [Key, Seconds, Value], State) ->
  case edis_util:binary_to_integer(Seconds) of
    Secs when Secs =< 0 ->
      tcp_err("invalid expire time in SETEX", State);
    Secs ->
      ok = edis_db:set_ex(State#state.db, Key, Secs, Value),
      tcp_ok(State)
  end;
run_command(<<"SETEX">>, _, State) ->
  tcp_err("wrong number of arguments for 'SETEX' command", State);
run_command(<<"SETNX">>, [Key, Value], State) ->
  try edis_db:set_nx(State#state.db, Key, Value) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run_command(<<"SETNX">>, _, State) ->
  tcp_err("wrong number of arguments for 'SETNX' command", State);
run_command(<<"SETRANGE">>, [Key, Offset, Value], State) ->
  case edis_util:binary_to_integer(Offset) of
    Off when Off < 0 ->
      tcp_err("offset is out of range", State);
    Off ->
      tcp_number(edis_db:set_range(State#state.db, Key, Off, Value), State)
  end;
run_command(<<"SETRANGE">>, _, State) ->
  tcp_err("wrong number of arguments for 'SETRANGE' command", State);
run_command(<<"STRLEN">>, [Key], State) ->
  tcp_number(edis_db:str_len(State#state.db, Key), State);
run_command(<<"STRLEN">>, _, State) ->
  tcp_err("wrong number of arguments for 'STRLEN' command", State);

%% -- Keys -----------------------------------------------------------------------------------------
run_command(<<"DEL">>, [], State) ->
  tcp_err("wrong number of arguments for 'DEL' command", State);
run_command(<<"DEL">>, Keys, State) ->
  tcp_number(edis_db:del(State#state.db, Keys), State);
run_command(<<"EXISTS">>, [Key], State) ->
  tcp_boolean(edis_db:exists(State#state.db, Key), State);
run_command(<<"EXISTS">>, _, State) ->
  tcp_err("wrong number of arguments for 'EXISTS' command", State);
run_command(<<"EXPIRE">>, [Key, Seconds], State) ->
  tcp_boolean(edis_db:expire(State#state.db, Key, edis_util:binary_to_integer(Seconds)), State);
run_command(<<"EXPIRE">>, _, State) ->
  tcp_err("wrong number of arguments for 'EXPIRE' command", State);
run_command(<<"EXPIREAT">>, [Key, Timestamp], State) ->
  tcp_boolean(edis_db:expire_at(State#state.db, Key, edis_util:binary_to_integer(Timestamp)), State);
run_command(<<"EXPIREAT">>, _, State) ->
  tcp_err("wrong number of arguments for 'EXPIREAT' command", State);
run_command(<<"KEYS">>, [Pattern], State) ->
  tcp_multi_bulk(edis_db:keys(State#state.db, edis_util:glob_to_re(Pattern)), State);
run_command(<<"KEYS">>, _, State) ->
  tcp_err("wrong number of arguments for 'KEYS' command", State);
run_command(<<"MOVE">>, [Key, Db], State) ->
  DbIndex = edis_util:binary_to_integer(Db, 0),
  case {DbIndex, edis_config:get(databases), State#state.db, edis_db:process(DbIndex)} of
    {DbIndex, Dbs, _, _} when DbIndex < 0 orelse DbIndex > Dbs ->
      tcp_err("index out of range", State);
    {_, _, CurrentDb, CurrentDb} ->
      tcp_err("source and destinantion objects are the same", State);
    {_, _, CurrentDb, DestDb} ->
      tcp_boolean(edis_db:move(CurrentDb, Key, DestDb), State)
  end;
run_command(<<"MOVE">>, _, State) ->
  tcp_err("wrong number of arguments for 'MOVE' command", State);
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
run_command(<<"OBJECT", _Rest/binary>>, _, State) ->
  tcp_err("Syntax error. Try OBJECT (refcount|encoding|idletime)", State);
run_command(<<"PERSIST">>, [Key], State) ->
  tcp_boolean(edis_db:persist(State#state.db, Key), State);
run_command(<<"PERSIST">>, _, State) ->
  tcp_err("wrong number of arguments for 'PERSIST' command", State);
run_command(<<"RANDOMKEY">>, [], State) ->
  tcp_bulk(edis_db:random_key(State#state.db), State);
run_command(<<"RANDOMKEY">>, _, State) ->
  tcp_err("wrong number of arguments for 'RANDOMKEY' command", State);
run_command(<<"RENAME">>, [Key, Key], State) ->
  tcp_err("source and destination objects are the same", State);
run_command(<<"RENAME">>, [Key, NewKey], State) ->
  try edis_db:rename(State#state.db, Key, NewKey) of
    ok -> tcp_ok(State)
  catch
    _:not_found ->
      tcp_err("no such key", State)
  end;
run_command(<<"RENAME">>, _, State) ->
  tcp_err("wrong number of arguments for 'RENAME' command", State);
run_command(<<"RENAMENX">>, [Key, Key], State) ->
  tcp_err("source and destination objects are the same", State);
run_command(<<"RENAMENX">>, [Key, NewKey], State) ->
  try edis_db:rename_nx(State#state.db, Key, NewKey) of
      ok -> tcp_number(1, State)
  catch
    _:already_exists ->
      tcp_number(0, State);
    _:not_found ->
      tcp_err("no such key", State)
  end;
run_command(<<"RENAMENX">>, _, State) ->
  tcp_err("wrong number of arguments for 'RENAME' command", State);
run_command(<<"TTL">>, [Key], State) ->
  try edis_db:ttl(State#state.db, Key) of
    undefined -> tcp_number(-1, State);
    Secs -> tcp_number(Secs, State)
  catch
    _:not_found ->
      tcp_number(-1, State)
  end;
run_command(<<"TTL">>, _, State) ->
  tcp_err("wrong number of arguments for 'TTL' command", State);
run_command(<<"TYPE">>, [Key], State) ->
  try edis_db:type(State#state.db, Key) of
    Type -> tcp_ok(atom_to_binary(Type, utf8), State)
  catch
    _:not_found ->
      tcp_ok(<<"none">>, State)
  end;
run_command(<<"TYPE">>, _, State) ->
  tcp_err("wrong number of arguments for 'TYPE' command", State);

%% -- Hashes ---------------------------------------------------------------------------------------
run_command(<<"HDEL">>, [], State) ->
  tcp_err("wrong number of arguments for 'HDEL' command", State);
run_command(<<"HDEL">>, [_Key], State) ->
  tcp_err("wrong number of arguments for 'HDEL' command", State);
run_command(<<"HDEL">>, [Key | Fields], State) ->
  tcp_number(edis_db:hdel(State#state.db, Key, Fields), State);
run_command(<<"HEXISTS">>, [Key, Field], State) ->
  tcp_boolean(edis_db:hexists(State#state.db, Key, Field), State);
run_command(<<"HEXISTS">>, _, State) ->
  tcp_err("wrong number of arguments for 'HEXISTS' command", State);
run_command(<<"HGET">>, [Key, Field], State) ->
  tcp_bulk(edis_db:hget(State#state.db, Key, Field), State);
run_command(<<"HGET">>, _, State) ->
  tcp_err("wrong number of arguments for 'HGET' command", State);
run_command(<<"HGETALL">>, [Key], State) ->
  tcp_multi_bulk(lists:flatmap(fun tuple_to_list/1, edis_db:hget_all(State#state.db, Key)), State);
run_command(<<"HGETALL">>, _, State) ->
  tcp_err("wrong number of arguments for 'HGET' command", State);
run_command(<<"HINCRBY">>, [Key, Field, Increment], State) ->
  Inc = edis_util:binary_to_integer(Increment),
  try 
    tcp_number(edis_db:hincr(State#state.db, Key, Field, Inc), State)
  catch
    _:not_integer ->
      tcp_err("hash value is not an integer", State)
  end;
run_command(<<"HINCRBY">>, _, State) ->
  tcp_err("wrong number of arguments for 'HINCRBY' command", State);
run_command(<<"HKEYS">>, [Key], State) ->
  tcp_multi_bulk(edis_db:hkeys(State#state.db, Key), State);
run_command(<<"HKEYS">>, _, State) ->
  tcp_err("wrong number of arguments for 'HKEYS' command", State);
run_command(<<"HLEN">>, [Key], State) ->
  tcp_number(edis_db:hlen(State#state.db, Key), State);
run_command(<<"HLEN">>, _, State) ->
  tcp_err("wrong number of arguments for 'HLEN' command", State);
run_command(<<"HMGET">>, [Key | Fields], State) when Fields =/= [] ->
  tcp_multi_bulk(edis_db:hget(State#state.db, Key, Fields), State);
run_command(<<"HMGET">>, _, State) ->
  tcp_err("wrong number of arguments for HMGET", State);
run_command(<<"HMSET">>, [Key | FVs], State) when FVs =/= [], length(FVs) rem 2 =:= 0 ->
  _ = edis_db:hset(State#state.db, Key, edis_util:make_pairs(FVs)),
  tcp_ok(State);
run_command(<<"HMSET">>, _, State) ->
  tcp_err("wrong number of arguments for HMSET", State);
run_command(<<"HSET">>, [Key, Field, Value], State) ->
  case edis_db:hset(State#state.db, Key, Field, Value) of
    inserted ->
      tcp_number(1, State);
    updated ->
      tcp_number(0, State)
  end;
run_command(<<"HSET">>, _, State) ->
  tcp_err("wrong number of arguments for 'HSET' command", State);
run_command(<<"HSETNX">>, [Key, Field, Value], State) ->
  try edis_db:hset_nx(State#state.db, Key, Field, Value) of
    ok -> tcp_boolean(true, State)
  catch
    _:already_exists ->
      tcp_boolean(false, State)
  end;
run_command(<<"HSETNX">>, _, State) ->
  tcp_err("wrong number of arguments for 'HSETNX' command", State);
run_command(<<"HVALS">>, [Key], State) ->
  tcp_multi_bulk(edis_db:hvals(State#state.db, Key), State);
run_command(<<"HVALS">>, _, State) ->
  tcp_err("wrong number of arguments for 'HVALS' command", State);

%% -- Lists ----------------------------------------------------------------------------------------
run_command(<<"BRPOPLPUSH">>, [Source, Destination, Timeout], State) ->
  try
    case edis_util:binary_to_integer(Timeout) of
      T when T < 0 ->
        tcp_err("timeout is negative", State);
      0 ->
        tcp_bulk(edis_db:brpop_lpush(State#state.db, Source, Destination, infinity), State);
      T ->
        tcp_bulk(edis_db:brpop_lpush(State#state.db, Source, Destination, T), State)
    end
  catch
    _:timeout ->
      tcp_bulk(undefined, State);
    _:not_integer ->
      tcp_err("timeout is not an integer or out of range", State)
  end;
run_command(<<"BRPOPLPUSH">>, _, State) ->
  tcp_err("wrong number of arguments for 'BRPOPLPUSH' command", State);
run_command(<<"LINDEX">>, [Key, Index], State) ->
  tcp_bulk(edis_db:lindex(State#state.db, Key, edis_util:binary_to_integer(Index, 0)), State);
run_command(<<"LINDEX">>, _, State) ->
  tcp_err("wrong number of arguments for 'LINDEX' command", State);
run_command(<<"LINSERT">>, [Key, Position, Pivot, Value], State) ->
  try
    case edis_util:upper(Position) of
      <<"BEFORE">> ->
        tcp_number(edis_db:linsert(State#state.db, Key, before, Pivot, Value), State);
      <<"AFTER">> ->
        tcp_number(edis_db:linsert(State#state.db, Key, 'after', Pivot, Value), State);
      _ ->
        tcp_err("syntax error", State)
    end
  catch
    _:not_found ->
      tcp_number(-1, State)
  end;
run_command(<<"LINSERT">>, _, State) ->
  tcp_err("wrong number of arguments for 'LINSERT' command", State);
run_command(<<"LLEN">>, [Key], State) ->
  tcp_number(edis_db:llen(State#state.db, Key), State);
run_command(<<"LLEN">>, _, State) ->
  tcp_err("wrong number of arguments for 'LLEN' command", State);
run_command(<<"LPOP">>, [Key], State) ->
  try edis_db:lpop(State#state.db, Key) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run_command(<<"LPOP">>, _, State) ->
  tcp_err("wrong number of arguments for 'LPOP' command", State);
run_command(<<"LPUSH">>, [Key, Value], State) ->
  tcp_number(edis_db:lpush(State#state.db, Key, Value), State);
run_command(<<"LPUSH">>, _, State) ->
  tcp_err("wrong number of arguments for 'LPUSH' command", State);
run_command(<<"LPUSHX">>, [Key, Value], State) ->
  try edis_db:lpush_x(State#state.db, Key, Value) of
    NewLen -> tcp_number(NewLen, State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run_command(<<"LPUSHX">>, _, State) ->
  tcp_err("wrong number of arguments for 'LPUSHX' command", State);
run_command(<<"LRANGE">>, [Key, Start, Stop], State) ->
  tcp_multi_bulk(edis_db:lrange(State#state.db, Key,
                                edis_util:binary_to_integer(Start, 0),
                                edis_util:binary_to_integer(Stop, 0)), State);
run_command(<<"LRANGE">>, _, State) ->
  tcp_err("wrong number of arguments for 'LRANGE' command", State);
run_command(<<"LREM">>, [Key, Count, Value], State) ->
  try edis_db:lrem(State#state.db, Key, edis_util:binary_to_integer(Count, 0), Value) of
    Removed ->
      tcp_number(Removed, State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run_command(<<"LREM">>, _, State) ->
  tcp_err("wrong number of arguments for 'LREM' command", State);
run_command(<<"LSET">>, [Key, Index, Value], State) ->
  try edis_db:lset(State#state.db, Key, edis_util:binary_to_integer(Index, 0), Value) of
    ok ->
      tcp_ok(State)
  catch
    _:not_found ->
      tcp_err("no such key", State);
    _:out_of_range ->
      tcp_err("index out of range", State)
  end;
run_command(<<"LSET">>, _, State) ->
  tcp_err("wrong number of arguments for 'LSET' command", State);
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
run_command(<<"LTRIM">>, _, State) ->
  tcp_err("wrong number of arguments for 'LTRIM' command", State);
run_command(<<"RPOP">>, [Key], State) ->
  try edis_db:rpop(State#state.db, Key) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run_command(<<"RPOP">>, _, State) ->
  tcp_err("wrong number of arguments for 'RPOP' command", State);
run_command(<<"RPOPLPUSH">>, [Source, Destination], State) ->
  try edis_db:rpop_lpush(State#state.db, Source, Destination) of
    Value -> tcp_bulk(Value, State)
  catch
    _:not_found ->
      tcp_bulk(undefined, State)
  end;
run_command(<<"RPOPLPUSH">>, _, State) ->
  tcp_err("wrong number of arguments for 'RPOPLPUSH' command", State);
run_command(<<"RPUSH">>, [Key, Value], State) ->
  tcp_number(edis_db:rpush(State#state.db, Key, Value), State);
run_command(<<"RPUSH">>, _, State) ->
  tcp_err("wrong number of arguments for 'RPUSH' command", State);
run_command(<<"RPUSHX">>, [Key, Value], State) ->
  try edis_db:rpush_x(State#state.db, Key, Value) of
    NewLen -> tcp_number(NewLen, State)
  catch
    _:not_found ->
      tcp_number(0, State)
  end;
run_command(<<"RPUSHX">>, _, State) ->
  tcp_err("wrong number of arguments for 'RPUSHX' command", State);



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
run_command(<<"CONFIG">>, _, State) ->
  tcp_err("wrong number of arguments for 'CONFIG' command", State);
run_command(<<"CONFIG GET">>, _, State) ->
  tcp_err("wrong number of arguments for CONFIG GET", State);
run_command(<<"CONFIG SET">>, [Key | Values], State) ->
  Param = binary_to_atom(edis_util:lower(Key), utf8),
  Value =
    case {Param, Values} of
      {listener_port_range, [P1, P2]} ->
        {edis_util:binary_to_integer(P1),
         edis_util:binary_to_integer(P2)};
      {listener_port_range, _} ->
        wrong_args;
      {client_timeout, [Timeout]} ->
        edis_util:binary_to_integer(Timeout);
      {client_tiemout, _} ->
        wrong_args;
      {databases, [Dbs]} ->
        edis_util:binary_to_integer(Dbs);
      {databases, _} ->
        wrong_args;
      {requirepass, []} ->
        undefined;
      {requirepass, [Pass]} ->
        Pass;
      {requirepass, _} ->
        wrong_args;
      {Param, [V]} ->
        V;
      {Param, Vs} ->
        Vs
    end,
  case Value of
    wrong_args ->
      tcp_err("wrong number of arguments for CONFIG SET", State);
    Value ->
      try edis_config:set(Param, Value) of
        ok -> tcp_ok(State)
      catch
        _:invalid_param ->
          tcp_err(["Invalid argument '", Value, "' for CONFIG SET '", Key, "'"], State);
        _:unsupported_param ->
          tcp_err(["Unsopported CONFIG parameter: ", Key], State)
      end
  end;
run_command(<<"CONFIG SET">>, _, State) ->
  tcp_err("wrong number of arguments for CONFIG SET", State);
run_command(<<"CONFIG RESETSTAT">>, [], State) ->
  %%TODO: Reset the statistics
  tcp_ok(State);
run_command(<<"CONFIG RESETSTAT">>, _, State) ->
  tcp_err("wrong number of arguments for CONFIG RESETSTAT", State);
run_command(<<"DBSIZE">>, [], State) ->
  tcp_number(edis_db:size(State#state.db), State);
run_command(<<"DBSIZE">>, _, State) ->
  tcp_err("wrong number of arguments for 'DBSIZE' command", State);
run_command(<<"FLUSHALL">>, [], State) ->
  ok = edis_db:flush(),
  tcp_ok(State);
run_command(<<"FLUSHALL">>, _, State) ->
  tcp_err("wrong number of arguments for 'FLUSHALL' command", State);
run_command(<<"FLUSHDB">>, [], State) ->
  ok = edis_db:flush(State#state.db),
  tcp_ok(State);
run_command(<<"FLUSHDB">>, _, State) ->
  tcp_err("wrong number of arguments for 'FLUSHDB' command", State);
run_command(<<"INFO">>, [], State) ->
  Info = edis_db:info(State#state.db),
  tcp_bulk(lists:map(fun({K,V}) when is_binary(V) ->
                             io_lib:format("~p:~s~n", [K, V]);
                        ({K,V}) ->
                             io_lib:format("~p:~p~n", [K, V])
                     end, Info), State);
run_command(<<"INFO">>, _, State) ->
  tcp_err("wrong number of arguments for 'INFO' command", State);
run_command(<<"LASTSAVE">>, [], State) ->
  Ts = edis_db:last_save(State#state.db),
  tcp_number(erlang:round(Ts), State);
run_command(<<"LASTSAVE">>, _, State) ->
  tcp_err("wrong number of arguments for 'LASTSAVE' command", State);
run_command(<<"MONITOR">>, [], State) ->
  ok = edis_db_monitor:add_sup_handler(),
  tcp_ok(State);
run_command(<<"MONITOR">>, _, State) ->
  tcp_err("wrong number of arguments for 'MONITOR' command", State);
run_command(<<"SAVE">>, [], State) ->
  ok = edis_db:save(State#state.db),
  tcp_ok(State);
run_command(<<"SAVE">>, _, State) ->
  tcp_err("wrong number of arguments for 'SAVE' command", State);
run_command(<<"SHUTDOWN">>, [], State) ->
  _ = spawn(edis, stop, []),
  {stop, normal, State};
run_command(<<"SHUTDOWN">>, _, State) ->
  tcp_err("wrong number of arguments for 'SHUTDOWN' command", State);
run_command(Command, Args, State)
  when Command == <<"SYNC">> orelse Command == <<"SLOWLOG">>
  orelse Command == <<"SLAVEOF">> ->
  ?WARN("Unsupported command: ~s~p~n", [Command, Args]),
  tcp_err("unsupported command", State);

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
    fun(Line, {noreply, AccState}) ->
            tcp_bulk(Line, AccState);
       (_Line, Error) ->
            Error
    end, tcp_send(["*", integer_to_list(erlang:length(Lines))], State), Lines).

%% @private
-spec tcp_bulk(undefined | iodata(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
tcp_bulk(undefined, State) ->
  tcp_send("$-1", State);
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