%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis Database Monitor
%%% @end
%%%-------------------------------------------------------------------
-module(edis_db_monitor).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").

-behaviour(gen_event).

-export([start_link/0, add_sup_handler/0, delete_handler/0, notify/1]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {client :: pid()}).
-type state() :: #state{}.

%% ====================================================================
%% External functions
%% ====================================================================
%% @doc Starts the event manager
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
  gen_event:start_link({local, ?MODULE}).

%% @doc Subscribes client.
%%      From this point on, all db operations will be notified to the client procedure using
%%      Erlang messages in the form of #edis_command{}.
%%      If the event handler later is deleted, the event manager sends a message {gen_event_EXIT,Handler,Reason} to the calling process. Reason is one of the following:
%%
%%        normal, if the event handler has been removed due to a call to delete_handler/3, or remove_handler has been returned by a callback function (see below).
%%        shutdown, if the event handler has been removed because the event manager is terminating.
%%        {swapped,NewHandler,Pid}, if the process Pid has replaced the event handler with another event handler NewHandler using a call to swap_handler/3 or swap_sup_handler/3.
%%        a term, if the event handler is removed due to an error. Which term depends on the error.
%% @end
-spec add_sup_handler() -> ok.
add_sup_handler() ->
  Self = self(),
  gen_event:add_sup_handler(?MODULE, {?MODULE, Self}, Self).

%% @doc Unsubscribes client.
-spec delete_handler() -> ok.
delete_handler() ->
  gen_event:delete_handler(?MODULE, {?MODULE, self()}, normal).

%% @doc Notifies an event.
-spec notify(#edis_command{}) -> ok.
notify(Command) ->
  gen_event:notify(?MODULE, Command).

%% ====================================================================
%% Server functions
%% ====================================================================
%% @hidden
-spec init(pid()) -> {ok, state()}.
init(Client) -> {ok, #state{client = Client}}.

%% @hidden
-spec handle_event(term(), state()) -> {ok, state()}.
handle_event(Event, State = #state{client = Client}) ->
  Client ! Event,
  {ok, State}.

%% @hidden
-spec handle_call(term(), state()) -> {ok, ok, state()}.
handle_call(_Request, State) -> {ok, ok, State}.
%% @hidden
-spec handle_info(term(), state()) -> {ok, state()}.
handle_info(Info, State) ->
  ?WARN("Unexpected Info:~n\t~p~n", [Info]),
  {ok, State}.

%% @hidden
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

%% @hidden
-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
