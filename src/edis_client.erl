%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis client FSM
%%% @end
%%%-------------------------------------------------------------------
-module(edis_client).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(gen_fsm).

-export([start_link/0, set_socket/2]).
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([wait_for_socket/2, args_or_oldcmd/2,bytes_in_command/2,get_command/2, 
         handle_command/2,recv_argument/2,running/2]).
-export([send/2, disconnect/1]).

-include("edis.hrl").
-define(FSM_TIMEOUT, 60000).

-record(state, {socket    :: port(),
                peerport  :: integer(),
                nargs     :: integer(),
                nextlength:: integer(),
                command   :: list(),
                args      :: list()
               }).

%% ====================================================================
%% External functions
%% ====================================================================
%% -- General ---------------------------------------------------------
%% @hidden
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_fsm:start_link(?MODULE, [], []).

%% @hidden
-spec set_socket(pid(), port()) -> ok.
set_socket(Client, Socket) ->
  gen_fsm:send_event(Client, {socket_ready, Socket}).

-spec send(pid(), term()) -> ok.
send(Client, Event) ->
  gen_fsm:send_event(Client, {send, Event}).

-spec disconnect(pid()) -> ok.
disconnect(Client) ->
  gen_fsm:send_event(Client, disconnect).

%% ====================================================================
%% Server functions
%% ====================================================================
%% @hidden
-spec init([]) -> {ok, wait_for_socket, #state{}, ?FSM_TIMEOUT}.
init([]) ->
  {ok, wait_for_socket, #state{}, ?FSM_TIMEOUT}.

%% ASYNC EVENTS -------------------------------------------------------
%% @hidden
-spec wait_for_socket({socket_ready, port()} | timeout | term(), #state{}) -> {next_state, wait_for_socket, #state{}, ?FSM_TIMEOUT} | {stop, timeout, #state{}}.
wait_for_socket({socket_ready, Socket}, State) ->
  % Now we own the socket
  PeerPort =
    case inet:peername(Socket) of
      {ok, {_Ip, Port}} -> Port;
      Error -> Error
    end,
  ?INFO("New Client: ~p ~n", [PeerPort]),
  ok = inet:setopts(Socket, [{active, once}, {packet, line}, binary]),
  {next_state, args_or_oldcmd, State#state{socket   = Socket,
                                             peerport = PeerPort}};
wait_for_socket(timeout, State) ->
  ?THROW("Timeout~n", []),
  {stop, timeout, State};
wait_for_socket(Other, State) ->
  ?THROW("Unexpected message: ~p\n", [Other]),
  {next_state, wait_for_socket, State, ?FSM_TIMEOUT}.

%% @hidden
-spec args_or_oldcmd(term(), #state{}) -> {next_state, bytes_in_command, #state{}} | {stop, timeout, #state{}} | {stop, {unexpected_event, term()}, #state{}}.
args_or_oldcmd({data, <<"*",N/binary>>}, State) ->
                 {NArgs,"\r\n"} = string:to_integer(binary_to_list(N)),
                 ?DEBUG("args: ~p ~n",[NArgs]),
                 NewState = State#state{nargs = NArgs, args=[]},
  {next_state, bytes_in_command, NewState};

args_or_oldcmd({data, <<"GET",Key/binary>>}, State = #state{socket = Socket}) ->
  case string:tokens(binary_to_list(Key),"\r\n") of
    [] -> err(Socket,"wrong number of arguments for 'get' command", State);
    [K] -> ?DEBUG("~p~n",[K]), handle_command(<<"GET">>,State#state{args=[K]})
  end,
  {next_state, args_or_oldcmd, State};

args_or_oldcmd(timeout, State) ->
    ?THROW("Timeout~n", []),
    {stop, timeout, State};
args_or_oldcmd(Event, State) ->
    ?THROW("Unexpected Event: ~p~n", [Event]),
    {stop, {unexpected_event, Event}, State}.

%% @hidden
-spec bytes_in_command(term(), #state{}) -> {next_state, get_command, #state{}} | {stop, timeout, #state{}} | {stop, {unexpected_event, term()}, #state{}}.
bytes_in_command({data, <<"$",N/binary>>}, State) ->
                 {Length,"\r\n"} = string:to_integer(binary_to_list(N)),
                 ?DEBUG("length: ~p ~n",[Length]),
                 NewState = State#state{nextlength = Length},
  {next_state, get_command, NewState};
bytes_in_command(timeout, State) ->
    ?THROW("Timeout~n", []),
    {stop, timeout, State};
bytes_in_command(Event, State) ->
    ?THROW("Unexpected Event: ~p~n", [Event]),
    {stop, {unexpected_event, Event}, State}.

%% @hidden
-spec get_command(term(), #state{}) -> {next_state, recv_argument, #state{}} | {stop, timeout, #state{}} | {stop, {unexpected_event, term()}, #state{}}.
get_command({data, Data}, State = #state{nextlength = Length}) ->
                 <<Command:Length/binary,13,10>> = Data,
                 ?DEBUG("Command: ~p ~n",[Command]),
                 NewState = State#state{nextlength  = undefined,
                                        command     = Command,
                                        nargs       = State#state.nargs - 1},
                 case NewState#state.nargs of
                   A when A > 1 ->
                     {next_state, recv_argument, NewState};
                   _ ->
                     proc_lib:spawn(?MODULE, handle_command, [Command, NewState]),
                     {next_state, args_or_oldcmd, NewState}
                end;
get_command(timeout, State) ->
    ?THROW("Timeout~n", []),
    {stop, timeout, State};
get_command(Event, State) ->
    ?THROW("Unexpected Event: ~p~n", [Event]),
    {stop, {unexpected_event, Event}, State}.

%% @hidden
-spec recv_argument(term(), #state{}) -> {next_state, running, #state{}} | {stop, timeout, #state{}} | {stop, {unexpected_event, term()}, #state{}}.
recv_argument({data, <<"$",N/binary>>}, State = #state{socket = Socket, command = Command, nargs = Nargs}) ->
                 {Length,"\r\n"} = string:to_integer(binary_to_list(N)),
                 ?DEBUG("length: ~p ~n",[Length]),
                 inet:setopts(Socket, [{packet,0}]),
                 {ok, <<Argument:Length/binary,"\r\n">>} = gen_tcp:recv(Socket, Length + 2, ?FSM_TIMEOUT),
                 ?DEBUG("packet: ~p ~n",[Argument]),
                 inet:setopts(Socket, [{active, once},{packet,line}]),
                 NewState = State#state{nextlength = Length, args = State#state.args ++ [Argument], nargs = Nargs - 1},
                 case Nargs of 
                   A when A > 1 -> 
                    {next_state, recv_argument, NewState};
                   _ -> 
                     proc_lib:spawn(?MODULE, handle_command, [Command, NewState]),
                    {next_state, args_or_oldcmd, NewState}
                end;
                   
recv_argument(timeout, State) ->
    ?THROW("Timeout~n", []),
    {stop, timeout, State};
recv_argument(Event, State) ->
    ?THROW("Unexpected Event: ~p~n", [Event]),
    {stop, {unexpected_event, Event}, State}.

%% @hidden
-spec handle_command(binary(), #state{}) -> term().
handle_command(<<"PING">>, State) ->
  tcp_send(State#state.socket, <<"+PONG",13,10>>, State),
  ok;
handle_command(Command, #state{args = Args}) ->
  ?WARN("Unknown command: ~s~p~n", [Command, Args]).

%% @hidden
-spec running(term(), #state{}) -> {next_state, running, #state{}} | {stop, normal | {unexpected_event, term()}, #state{}}.
running({send, Message}, State = #state{socket = S}) ->
  ?DEBUG("Sending: ~p~n", [Message]),
  ok = tcp_send(S, frame(Message), State),
  {next_state, running, State};
running(disconnect, State) ->
  ?DEBUG("disconnecting...~n", []),
  {stop, normal, State};
running(Event, State) ->
  ?THROW("Unexpected Event:~n\t~p~n", [Event]),
  {stop, {unexpected_event, Event}, State}.

%% OTHER EVENTS -------------------------------------------------------
%% @hidden
-spec handle_event(X, atom(), #state{}) -> {stop, {atom(), undefined_event, X}, #state{}}.
handle_event(Event, StateName, StateData) ->
    {stop, {StateName, undefined_event, Event}, StateData}.

%% @hidden
-spec handle_sync_event(X, reference(), atom(), #state{}) -> {stop, {atom(), undefined_event, X}, #state{}}.
handle_sync_event(Event, _From, StateName, StateData) ->
    {stop, {StateName, undefined_event, Event}, StateData}.

%% @hidden
-spec handle_info(term(), atom(), #state{}) -> term().
handle_info({tcp, Socket, Bin}, StateName, #state{socket = Socket,
                                                  peerport = PeerPort} = StateData) ->
    % Flow control: enable forwarding of next TCP message
    ok = inet:setopts(Socket, [{active, false}]),
    ?DEBUG("Data received from ~p: ~s", [PeerPort, Bin]),
    Result = ?MODULE:StateName({data, Bin}, StateData),
    ok = inet:setopts(Socket, [{active, once}]),
    Result;
handle_info({tcp_closed, Socket}, _StateName, #state{socket = Socket,
                                                     peerport = PeerPort} = StateData) ->
    ?DEBUG("Disconnected ~p.~n", [PeerPort]),
    {stop, normal, StateData};
handle_info(_Info, StateName, StateData) ->
    {next_state, StateName, StateData}.

%% @hidden
-spec terminate(term(), atom(), #state{}) -> ok.
terminate(normal, _StateName, #state{socket = Socket}) ->
  (catch gen_tcp:close(Socket)),
  ok;
terminate(Reason, _StateName, #state{socket = Socket}) ->
  ?WARN("Terminating client: ~p~n", [Reason]),
  (catch gen_tcp:close(Socket)),
  ok.

%% @hidden
-spec code_change(term(), atom(), #state{}, any()) -> {ok, atom(), #state{}}.
code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%% ====================================================================
%% Internal functions
%% ====================================================================
%% @spec tcp_send(port(), binary(), term()) -> ok
%% @doc  Sends a message through TCP socket or fails gracefully 
%%       (in a gen_fsm fashion)
-spec tcp_send(port(), binary(), term()) -> ok.
tcp_send(Socket, Message, State) ->
  try gen_tcp:send(Socket, Message) of
    ok ->
      ok;
    {error, closed} ->
      ?DEBUG("Connection closed~n", []),
      throw({stop, normal, State});
    {error, Error} ->
      ?THROW("Couldn't send msg through TCP~n\tError: ~p~n", [Error]),
      throw({stop, {error, Error}, State})
  catch
    _:{Exception, _} ->
      ?THROW("Couldn't send msg through TCP~n\tError: ~p~n", [Exception]),
      throw({stop, normal, State});
    _:Exception ->
      ?THROW("Couldn't send msg through TCP~n\tError: ~p~n", [Exception]),
      throw({stop, normal, State})
  end.

err(Socket, Message, State) -> 
  tcp_send(Socket,"-ERR " ++ Message ++ "\r\n", State).

frame(List) when is_list(List) -> 
  frame(list_to_binary(List));

frame(Bin) -> 
  Length = size(Bin),
  Result = <<0,Length:8/little-unsigned-integer-unit:8,Bin/binary,255>>,
  ?DEBUG("client sending ~p bytes:~n\t~s~n", [Length, Bin]),
  Result.

