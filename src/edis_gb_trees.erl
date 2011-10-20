%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Extension for gb_trees with a couple of extra functions
%%% @end
%%%-------------------------------------------------------------------
-module(edis_gb_trees).
-extends(gb_trees).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-export([rev_iterator/1, previous/1]).

-type edis_gb_tree() :: gb_tree().

%% @doc Returns a reverse iterator. It's just like the one returned by {@link iterator/1} but it
%%      traverses the tree in the exact opposite direction
-spec rev_iterator(edis_gb_tree()) -> [gb_trees:gb_tree_node()].
rev_iterator({_, T}) ->
    rev_iterator_1(T).

rev_iterator_1(T) ->
    rev_iterator(T, []).

%% The rev_iterator structure is really just a list corresponding to
%% the call stack of an post-order traversal. This is quite fast.

rev_iterator({_, _, _, nil} = T, As) ->
    [T | As];
rev_iterator({_, _, _, R} = T, As) ->
    rev_iterator(R, [T | As]);
rev_iterator(nil, As) ->
    As.

%% @doc Like {@link next/1} for reverse iterators
-spec previous([gb_trees:gb_tree_node()]) -> 'none' | {term(), term(), [gb_trees:gb_tree_node()]}.
previous([{X, V, T, _} | As]) ->
    {X, V, rev_iterator(T, As)};
previous([]) ->
    none.