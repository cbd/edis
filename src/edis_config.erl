%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis Configuration utilities
%%% @end
%%%-------------------------------------------------------------------
-module(edis_config).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-export([get/1]).

-include("edis.hrl").

-type config_option() :: listener_port_range | client_timeout | databases | requirepass.

-spec get(config_option()) -> term().
get(listener_port_range) ->
  get(listener_port_range, {6379,6379});
get(client_timeout) ->
  get(client_tiemout, 35000);
get(databases) ->
  get(databases, 16);
get(requirepass) ->
  get(requirepass, false).

get(Field, Default) ->
  case application:get_env(edis, Field) of
    {ok, Value} ->
      ?DEBUG("~p := ~p~n", [Field, Value]),
      Value;
    _ ->
      ?DEBUG("~p := ~p~n", [Field, Default]),
      Default
  end.