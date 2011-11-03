%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc DB backend behaviour for edis
%%% @end
%%%-------------------------------------------------------------------
-module(edis_backend).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").

-type write_actions() :: [{put, Key::binary(), Value::#edis_item{}} |
                          {delete, Key::binary()} |
                          clear].
-type fold_fun() :: fun((Item::#edis_item{}, any()) -> any()).
-export_type([write_actions/0, fold_fun/0]).

-export([behaviour_info/1]).

-spec behaviour_info(callbacks|term()) -> undefined | [{atom(), non_neg_integer()}].
behaviour_info(callbacks) -> [{init, 3}, {write, 2}, {put, 3}, {delete, 2}, {is_empty, 1},
                              {destroy, 1}, {status, 1}, {get, 2}];
behaviour_info(_) -> undefined.