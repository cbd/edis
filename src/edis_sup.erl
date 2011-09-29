%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Edis main supervisor
%%% @end
%%%-------------------------------------------------------------------
-module(edis_sup).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(supervisor).

-export([start_link/0, init/1]).

%%-------------------------------------------------------------------
%% PUBLIC API
%%-------------------------------------------------------------------
%% @doc  Starts a new supervisor
-spec start_link() -> {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%-------------------------------------------------------------------
%% SUPERVISOR API
%%-------------------------------------------------------------------
%% @hidden
-spec init([]) -> {ok, {{one_for_one, 5, 10}, [supervisor:child_spec()]}}.
init([]) ->
  ListenerSup = {edis_listener_sup, {edis_listener_sup, start_link, []},
                 permanent, 1000, supervisor, [edis_listener_sup]},
  ClientSup = {edis_client_sup, {edis_client_sup, start_link, []},
               permanent, 1000, supervisor, [edis_client_sup]},
  DbSup = {edis_db_sup, {edis_db_sup, start_link, []},
           permanent, 1000, supervisor, [edis_db_sup]},
  {ok, {{one_for_one, 5, 10}, [DbSup, ClientSup, ListenerSup]}}.