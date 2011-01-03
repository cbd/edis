%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @copyright (C) 2010 InakaLabs S.R.L.
%%% @doc Supervisor for Client Processes
%%% @end
%%%-------------------------------------------------------------------
-module(client_manager).

-behaviour(supervisor).

-export([start_link/0, start_client/0, init/1]).

-include("log.hrl").

%% ====================================================================
%% External functions
%% ====================================================================
%% @spec start_link() -> ignore | {error, term()} | {ok, pid()}
%% @doc  Starts the supervisor process
-spec start_link() -> ignore | {error, term()} | {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec start_client() -> {ok, pid() | undefined} | {error, term()}
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
    {ok,
        {_SupFlags = {simple_one_for_one, 100, 1},
            [
              % TCP Client
              {   undefined,                               % Id       = internal id
                  {client_handler, start_link, []},        % StartFun = {M, F, A}
                  temporary,                               % Restart  = permanent | transient | temporary
                  brutal_kill,                             % Shutdown = brutal_kill | int() >= 0 | infinity
                  worker,                                  % Type     = worker | supervisor
                  [client_handler]                         % Modules  = [Module] | dynamic
              }
            ]
        }
    }.

