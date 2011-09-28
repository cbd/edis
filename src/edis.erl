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

-include("edis.hrl").

-behaviour(application).

-export([start/0, stop/0]).
-export([define_command/1]).
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
%% USER API
%%-------------------------------------------------------------------
%% @doc Describes a command given its name
%% @todo Optimize (i.e. command list may be a huge one and we shouldn't write all those in here)
%%       We may want to store them on ets, mnesia, etc...
-spec define_command(binary()) -> undefined | #edis_command{}.
define_command(<<"PING">>) ->
  #edis_command{name = <<"PING">>, args = 0};
define_command(_) ->
  undefined.

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