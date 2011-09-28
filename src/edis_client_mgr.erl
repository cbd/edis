%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Edis client manager
%%% @end
%%%-------------------------------------------------------------------
-module(edis_client_mgr).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").

-behaviour(supervisor).

-export([start_link/1, start_client/0, init/1]).

%% ====================================================================
%% External functions
%% ====================================================================
%% @doc  Starts the supervisor process
-spec start_link(atom()) -> ignore | {error, term()} | {ok, pid()}.
start_link(Name) ->
	supervisor:start_link({local, Name}, ?MODULE, []).

%% @doc  Starts a new client process
-spec start_client() -> {ok, pid() | undefined} | {error, term()}.
start_client() ->
  supervisor:start_child(?MODULE, []).

%% ====================================================================
%% Server functions
%% ====================================================================
%% @hidden
-spec init([]) -> {ok, {{simple_one_for_one, 100, 1}, [supervisor:child_spec()]}}.
init([]) ->
  {ok, {{simple_one_for_one, 100, 1},
        [{edis_client, {edis_client, start_link, []},
          temporary, brutal_kill, worker, [edis_client]}]}}.