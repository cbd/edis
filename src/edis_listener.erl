%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Listener process for clients
%%% @reference See <a href="http://www.trapexit.org/index.php/Building_a_Non-blocking_TCP_server_using_OTP_principles">this article</a> for more information, and liberally borrowed from Fernando Benavides' code - fernando@inakanetworks.com
%%% @end
%%%-------------------------------------------------------------------
-module(edis_listener).
-author('Chad DePue <chad@inakanetworks.com>').
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').

-include("edis.hrl").

-behaviour(gen_server).

%% -------------------------------------------------------------------
%% Exported functions
%% -------------------------------------------------------------------
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TCP_OPTIONS,[binary,
                     {packet, line},
                     {keepalive, true},
                     {active, false},
                     {reuseaddr, true},
                     {nodelay, true}, %% We want to be informed even when packages are small
                     {backlog, 128000}, %% We don't care if we have logs of pending connections, we'll process them anyway
                     {send_timeout, edis_config:get(client_timeout)}, %% If we couldn't send a message in this interval, something is definitively wrong...
                     {send_timeout_close, true} %%... and therefore the connection should be closed
                     ]).

-record(state, {listener :: port(), % Listening socket
                acceptor :: term()  % Asynchronous acceptor's internal reference
               }).

%% ====================================================================
%% External functions
%% ====================================================================

%% @doc  Starts a new client listener
-spec start_link(pos_integer()) -> {ok, pid()}.
start_link(Port) -> 
  gen_server:start_link(
    {local, list_to_atom("edis-client-listener-" ++ integer_to_list(Port))}, ?MODULE, Port, []).

%% ====================================================================
%% Callback functions
%% ====================================================================
%% @hidden
-spec init(pos_integer()) -> {ok, #state{}} | {stop, term()}.
init(Port) ->
  case gen_tcp:listen(Port, ?TCP_OPTIONS) of
    {ok, Socket} ->
      {ok, Ref} = prim_inet:async_accept(Socket, -1),
      lager:info("Client listener initialized (listening on port ~p)~n", [Port]),
      {ok, #state{listener = Socket,
                  acceptor = Ref}};
    {error, Reason} ->
      lager:alert("Client listener couldn't listen to port ~p: ~p~n", [Port, Reason]),
      {stop, Reason}
    end.

%% @hidden
-spec handle_call(X, reference(), #state{}) -> {stop, {unknown_request, X}, {unknown_request, X}, #state{}}.
handle_call(Request, _From, State) ->
  {stop, {unknown_request, Request}, {unknown_request, Request}, State}.

%% @hidden
-spec handle_cast(any(), #state{}) -> {noreply, #state{}, hibernate}.
handle_cast(_Msg, State) -> {noreply, State, hibernate}.

%% @hidden
-spec handle_info(any(), #state{}) -> {noreply, #state{}, hibernate}.
handle_info({inet_async, ListSock, Ref, {ok, CliSocket}},
            #state{listener = ListSock, acceptor = Ref} = State) ->
  try
    PeerPort =
      case inet:peername(CliSocket) of
        {ok, {_Ip, Port}} -> Port;
        PeerErr -> PeerErr
      end,
    case set_sockopt(ListSock, CliSocket) of
      ok ->
        void;
      {error, Reason} ->
        exit({set_sockopt, Reason})
    end,
    
    %% New client connected - spawn a new process using the simple_one_for_one supervisor.
    lager:debug("Client ~p starting...~n", [PeerPort]),
    {ok, Pid} = edis_client_sup:start_client(),

    ok = gen_tcp:controlling_process(CliSocket, Pid),
    
    %% Instruct the new FSM that it owns the socket.
    ok = edis_client:set_socket(Pid, CliSocket),
    
    %% Signal the network driver that we are ready to accept another connection
    NewRef =
      case prim_inet:async_accept(ListSock, -1) of
        {ok, NR} ->
          NR;
        {error, Err} ->
          lager:alert("Couldn't accept: ~p~n", [inet:format_error(Err)]),
          exit({async_accept, inet:format_error(Err)})
      end,
    
    {noreply, State#state{acceptor = NewRef}}
  catch
    exit:Error ->
      lager:error("Error in async accept: ~p.\n", [Error]),
      {stop, Error, State}
  end;
handle_info({inet_async, ListSock, Ref, Error},
            #state{listener = ListSock, acceptor = Ref} = State) ->
  lager:alert("Error in socket acceptor: ~p.\n", [Error]),
  {stop, Error, State};
handle_info(_Info, State) ->
  {noreply, State, hibernate}.

%% @hidden
-spec terminate(any(), #state{}) -> any().
terminate(Reason, _State) ->
  lager:info("Listener terminated: ~p~n", [Reason]),
  ok.

%% @hidden
-spec code_change(any(), any(), any()) -> {ok, any()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
%% @doc Taken from prim_inet.  We are merely copying some socket options from the
%%      listening socket to the new client socket.
-spec set_sockopt(port(), port()) -> ok | {error, Reason :: term()}.
set_sockopt(ListSock, CliSocket) ->
  true = inet_db:register_socket(CliSocket, inet_tcp),
  case prim_inet:getopts(ListSock, [active, nodelay, keepalive, delay_send, priority, tos]) of
    {ok, Opts} ->
      case prim_inet:setopts(CliSocket, Opts) of
        ok ->
          ok;
        Error ->
          gen_tcp:close(CliSocket),
          Error
      end;
    Error ->
      gen_tcp:close(CliSocket),
      Error
  end.