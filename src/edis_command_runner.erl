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
handle_info(_, State) -> {noreply, State, hibernate}.

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
run_command(<<"SELECT">>, [Index], State) ->
  try {edis_util:binary_to_integer(Index), edis_config:get(databases)} of
    {Db, Dbs} when Db < 0 orelse Db >= Dbs ->
      tcp_err("invalid DB index", State);
    {Db, _} ->
      tcp_ok(State#state{db = edis_db:process(Db)})
  catch
    error:badarg ->
      ?WARN("Switching to db 0 because we received '~s' as the db index. This behaviour was copied from redis-server~n", [Index]),
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
    _:badarg ->
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
    ok -> tcp_number(1, State)
  catch
    _:already_exists ->
      tcp_number(0, State)
  end;
run_command(<<"MSETNX">>, _, State) ->
  tcp_err("wrong number of arguments for 'MSETNX' command", State);

run_command(<<"SET">>, [Key, Value], State) ->
  ok = edis_db:set(State#state.db, Key, Value),
  tcp_ok(State);
run_command(<<"SET">>, _, State) ->
  tcp_err("wrong number of arguments for 'SET' command", State);

%% -- Server ---------------------------------------------------------------------------------------
run_command(<<"CONFIG">>, [SubCommand | Rest], State) ->
  run_command(<<"CONFIG ", (edis_util:upper(SubCommand))/binary>>, Rest, State);
run_command(<<"CONFIG GET">>, [Pattern], State) ->
  Configs = edis_config:get(
              binary:replace(
                binary:replace(Pattern, <<"*">>, <<".*">>, [global]),
                <<"?">>, <<".">>, [global])),
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
-spec tcp_number(integer(), state()) -> {noreply, state()} | {stop, normal | {error, term()}, state()}.
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