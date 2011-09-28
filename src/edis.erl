%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Redis (http://redis.io), but in Erlang
%%% @end
%%%-------------------------------------------------------------------
-module(edis).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').
-vsn('0.1').

-behaviour(application).

%% Application callbacks
-export([start/0, stop/0]).
-export([start/2, stop/1]).

%%-------------------------------------------------------------------
%% ADMIN API
%%-------------------------------------------------------------------
%% @doc Starts the application
-spec start() -> ok | {error, {already_started, ?MODULE}}.
start() -> application:start(?MODULE).

%% @doc Stops the application
-spec stop() -> ok.
stop() -> application:stop(?MODULE).

%%-------------------------------------------------------------------
%% BEHAVIOUR CALLBACKS
%%-------------------------------------------------------------------
%% @private
-spec start(any(), any()) -> {ok, pid()}.
start(_StartType, _StartArgs) ->
  edis_sup:start_link().

%% @private
-spec stop(any()) -> ok.
stop(_State) -> ok.