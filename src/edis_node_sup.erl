%%%-------------------------------------------------------------------
%%% @author Joachim Nilsson <joachim@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Edis Node supervisor
%%% @end
%%%-------------------------------------------------------------------
-module(edis_node_sup).
-author('Joachim Nilsson <joachim@inakanetworks.com>').

-include("edis.hrl").

-behaviour(supervisor).

-export([start_link/0, reload/0, init/1]).

%% ====================================================================
%% External functions
%% ====================================================================
%% @doc  Starts the supervisor process
-spec start_link() -> ignore | {error, term()} | {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc  Reloads configuration. Restarts the managers
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
  lager:info("Node supervisor initialized~n", []),
  Mgr = {edis_node_monitor, {edis_node_monitor, start_link, []}, permanent, brutal_kill, worker, [edis_node_monitor]},
  {ok, {{one_for_one, 5, 1}, [Mgr]}}.