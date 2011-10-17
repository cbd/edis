%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Sorted Sets
%%% @end
%%%-------------------------------------------------------------------
-module(zsets).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-record(zset, {dict :: dict(),
               tree :: gb_tree()}).
-opaque zset(_Scores, _Values) :: #zset{}.

-opaque iterator(_Scores, _Values) :: gb_tree:iter().
-export_type([zset/2, iterator/2]).

-export([new/0, enter/2, enter/3, size/1]).
-export([iterator/1, next/1]).

%% @doc Creates an empty {@link zset(any(), any())}
-spec new() -> zset(any(), any()).
new() ->
  #zset{dict = dict:new(), tree = gb_trees:empty()}.

%% @equiv enter(Score, Value, ZSet)
-spec enter({Score, Value}, zset(Scores, Values)) -> zset(Scores, Values) when is_subtype(Score, Scores), is_subtype(Value, Values).
enter({Score, Value}, ZSet) ->
  enter(Score, Value, ZSet).

%% @doc Adds a new element to the zset.
%%      If the element is already present it justs updates its score
-spec enter(Score, Value, zset(Scores, Values)) -> zset(Scores, Values) when is_subtype(Score, Scores), is_subtype(Value, Values).
enter(Score, Value, ZSet = #zset{}) ->
  case dict:find(Value, ZSet#zset.dict) of
    error ->
      ZSet#zset{dict = dict:store(Value, Score, ZSet#zset.dict),
                tree = gb_trees:enter({Score, Value}, undefined, ZSet#zset.tree)};
    {ok, PrevScore} ->
      ZSet#zset{dict = dict:store(Value, Score, ZSet#zset.dict),
                tree = gb_trees:enter({Score, Value}, undefined,
                                      gb_trees:delete({PrevScore, Value}, ZSet#zset.tree))}
  end.

%% @doc Returns the size of the zset
-spec size(zset(any(), any())) -> non_neg_integer().
size(ZSet) ->
  gb_trees:size(ZSet#zset.tree).

%% @doc Returns an iterator that can be used for traversing the entries of Tree; see {@link next/1}.
-spec iterator(zset(Scores, Values)) -> iterator(Scores, Values).
iterator(ZSet) ->
  gb_trees:iterator(ZSet#zset.tree).

%% @doc Returns {Score, Value, Iter2} where Score is the smallest score referred to by the iterator
%% Iter1, and Iter2 is the new iterator to be used for traversing the remaining nodes, or the atom
%% none if no nodes remain.
 -spec next(iterator(Scores, Values)) -> none | {Scores, Values, iterator(Scores, Values)}.
next(Iter1) ->
  case gb_trees:next(Iter1) of
    none -> none;
    {{Score, Value}, _, Iter2} -> {Score, Value, Iter2}
  end.
 
x() -> y. 