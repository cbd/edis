%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Edis TCP listener supervisor
%%% @end
%%%-------------------------------------------------------------------
-module(edis_listener_sup).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").

-behaviour(supervisor).

-export([start_link/0, init/1, reload/0]).

%% ====================================================================
%% External functions
%% ====================================================================
%% @doc  Starts the supervisor process
-spec start_link() -> ignore | {error, term()} | {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc  Reloads configuration. Restarts the listeners
-spec reload() -> ok.
reload() ->
  true = exit(erlang:whereis(?MODULE), kill),
  ok.

%% ====================================================================
%% Server functions
%% ====================================================================
%% @hidden
-spec init([]) -> {ok, {{one_for_one, 5, 10}, [supervisor:child_spec()]}}.
init([]) ->
  lager:info("Listener supervisor initialized~n", []),
  {MinPort, MaxPort} = edis_config:get(listener_port_range),
  Listeners =
    [{list_to_atom("edis-listener-" ++ integer_to_list(I)),
      {edis_listener, start_link, [I]}, permanent, brutal_kill, worker, [edis_listener]}
     || I <- lists:seq(MinPort, MaxPort)],
  {ok, {{one_for_one, 5, 10}, Listeners}}.