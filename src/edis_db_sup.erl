%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Edis DataBase supervisor
%%% @end
%%%-------------------------------------------------------------------
-module(edis_db_sup).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").

-behaviour(supervisor).

-export([start_link/0, init/1]).

%% ====================================================================
%% External functions
%% ====================================================================
%% @doc  Starts the supervisor process
-spec start_link() -> ignore | {error, term()} | {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ====================================================================
%% Server functions
%% ====================================================================
%% @hidden
-spec init([]) -> {ok, {{one_for_one, 5, 10}, [supervisor:child_spec()]}}.
init([]) ->
  Databases = edis_config:get(databases),
  ?INFO("Client supervisor initialized (~p databases)~n", [Databases]),
  Monitor = {edis_db_monitor, {edis_db_monitor, start_link, []},
             permanent, brutal_kill, worker, [edis_db_monitor]},
  Children =
    [{edis_db:process(I), {edis_db, start_link, [I]},
      permanent, brutal_kill, supervisor, [edis_db]}
     || I <- lists:seq(0, Databases - 1)],
  {ok, {{one_for_one, 5, 10}, [Monitor | Children]}}.