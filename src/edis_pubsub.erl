%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis PubSub Monitor
%%% @end
%%%-------------------------------------------------------------------
-module(edis_pubsub).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").

-behaviour(gen_event).

-export([subscribe_channel/1, subscribe_pattern/1,
         unsubscribe_channel/1, unsubscribe_pattern/1,
         publish/2]).
-export([start_link/1, init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {client  :: pid(),
                type    :: pattern | channel,
                value   :: binary() | re:mp()}).
-type state() :: #state{}.

%% ====================================================================
%% External functions
%% ====================================================================
%% @doc Starts the event manager
-spec start_link(edis_pubsub_channel | edis_pubsub_pattern) -> {ok, pid()} | {error, term()}.
start_link(Name) ->
  gen_event:start_link({local, Name}).

%% @doc Subscribes client to channel.
%%      From this point on, all events on that channel will be notified to the client procedure using
%%      Erlang messages in the form of #edis_message{}.
%%      If the event handler later is deleted, the event manager sends a message {gen_event_EXIT,Handler,Reason} to the calling process. Reason is one of the following:
%%
%%        normal, if the event handler has been removed due to a call to delete_handler/3, or remove_handler has been returned by a callback function (see below).
%%        shutdown, if the event handler has been removed because the event manager is terminating.
%%        {swapped,NewHandler,Pid}, if the process Pid has replaced the event handler with another event handler NewHandler using a call to swap_handler/3 or swap_sup_handler/3.
%%        a term, if the event handler is removed due to an error. Which term depends on the error.
%% @end
-spec subscribe_channel(binary()) -> ok.
subscribe_channel(Channel) ->
  Subscription = {channel, Channel, self()},
  gen_event:add_sup_handler(edis_pubsub_channel, {?MODULE, Subscription}, Subscription).

%% @doc Subscribes client to pattern.
%%      From this point on, all events on channels that match the pattern will be notified to the client procedure using
%%      Erlang messages in the form of #edis_message{}.
%%      If the event handler later is deleted, the event manager sends a message {gen_event_EXIT,Handler,Reason} to the calling process. Reason is one of the following:
%%
%%        normal, if the event handler has been removed due to a call to delete_handler/3, or remove_handler has been returned by a callback function (see below).
%%        shutdown, if the event handler has been removed because the event manager is terminating.
%%        {swapped,NewHandler,Pid}, if the process Pid has replaced the event handler with another event handler NewHandler using a call to swap_handler/3 or swap_sup_handler/3.
%%        a term, if the event handler is removed due to an error. Which term depends on the error.
%% @end
-spec subscribe_pattern(binary()) -> ok.
subscribe_pattern(Pattern) ->
  Subscription = {pattern, Pattern, self()},
  gen_event:add_sup_handler(edis_pubsub_pattern, {?MODULE, Subscription}, Subscription).

%% @doc Unsubscribes client from channel.
-spec unsubscribe_channel(binary()) -> ok.
unsubscribe_channel(Channel) ->
  Subscription = {channel, Channel, self()},
  gen_event:delete_handler(edis_pubsub_channel, {?MODULE, Subscription}, normal).

%% @doc Unsubscribes client from pattern.
-spec unsubscribe_pattern(binary()) -> ok.
unsubscribe_pattern(Pattern) ->
  Subscription = {pattern, Pattern, self()},
  gen_event:delete_handler(edis_pubsub_pattern, {?MODULE, Subscription}, normal).

%% @doc Notifies an event.
%%      Returns the number of clients that received the message.
-spec publish(binary(), binary()) -> non_neg_integer().
publish(Channel, Message) ->
  ok = gen_event:notify(edis_pubsub_channel, #edis_message{channel = Channel, message = Message}),
  ok = gen_event:notify(edis_pubsub_pattern, #edis_message{channel = Channel, message = Message}),
  %%TODO: Optimize this
  Channels =
    erlang:length(
      [x || {channel, Ch, _} <- gen_event:which_handlers(edis_pubsub_channel), Ch =:= Channel]),
  Patterns =
    erlang:length(
      [x || {pattern, Re, _} <- gen_event:which_handlers(edis_pubsub_pattern), re:run(Channel, Re) =/= nomatch]),
  Channels + Patterns.

%% ====================================================================
%% Server functions
%% ====================================================================
%% @hidden
-spec init({channel|pattern, binary(), pid()}) -> {ok, state()}.
init({Type, Value, Client}) ->
  {ok, #state{client = Client,
              value  = case Type of
                         channel -> Value;
                         pattern -> re:compile(Value)
                       end,
              type   = Type}}.

%% @hidden
-spec handle_event(term(), state()) -> {ok, state()}.
handle_event(Message = #edis_message{channel = Channel}, State = #state{type = channel, value = Channel}) ->
  State#state.client ! Message,
  {ok, State};
handle_event(Message = #edis_message{}, State = #state{type = pattern}) ->
  case re:run(Message#edis_message.channel, State#state.value) of
    nomatch -> {ok, State};
    _ ->
      State#state.client ! Message,
      {ok, State}
  end;
handle_event(_Message, State) ->
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