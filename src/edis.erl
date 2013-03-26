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

-type result_type() :: ok | string | bulk | number | float | zrange | multi_bulk | boolean | multi_result | sort.
-type command() :: #edis_command{}.
-export_type([result_type/0, command/0]).

-behaviour(application).

-export([start/0, stop/0]).
-export([start/2, stop/1]).
-export([main/1]).

%%-------------------------------------------------------------------
%% ESCRIPT API
%%-------------------------------------------------------------------
%% @doc Entry point for escript
-spec main([string()]) -> ok.
main(Args) ->
  case Args of
    [] -> ok;
    [ConfigFile] -> edis_util:load_config(ConfigFile)
  end,
  crypto:start(),
  ok = application:start(lager),
  ok = start(),
  Pid = erlang:whereis(edis_sup),
  Ref = erlang:monitor(process, Pid),
  receive
    {'DOWN',Ref,process,Pid,shutdown} -> halt(0);
    {'DOWN',Ref,process,Pid,Reason} -> error_logger:error_msg("System down: ~p~n", [Reason]), halt(1);
    Error -> error_logger:error_msg("Unexpected message: ~p~n", [Error]), halt(-1)
  end.

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
%% Try to connect to a specific host - by choosing a cookie.
%% erlang:get_cookie() (of the current node)
%%  OtherNodes = net_adm:world(),
  %%Me = node(),
  %%erlang:set_cookie(Me, my_cookie),
  pg2:create(node_group),
  edis_sup:start_link().

%% @private
-spec stop(any()) -> ok.
stop(_State) -> ok.
