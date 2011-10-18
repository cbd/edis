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

-record(zset, {dict :: [{_,_}],
               tree :: gb_tree()}).
-opaque zset(_Scores, _Members) :: #zset{}.

-opaque iterator(_Scores, _Members) :: gb_tree:iter().
-export_type([zset/2, iterator/2]).

-export([new/0, enter/2, enter/3, size/1, find/2]).
-export([iterator/1, next/1, map/2]).
-export([intersection/2, intersection/3]).

%% @doc Creates an empty {@link zset(any(), any())}
-spec new() -> zset(any(), any()).
new() ->
  #zset{dict = orddict:new(), tree = gb_trees:empty()}.

%% @equiv enter(Score, Member, ZSet)
-spec enter({Score, Member}, zset(Scores, Members)) -> zset(Scores, Members) when is_subtype(Score, Scores), is_subtype(Member, Members).
enter({Score, Member}, ZSet) ->
  enter(Score, Member, ZSet).

%% @doc Adds a new element to the zset.
%%      If the element is already present it justs updates its score
-spec enter(Score, Member, zset(Scores, Members)) -> zset(Scores, Members) when is_subtype(Score, Scores), is_subtype(Member, Members).
enter(Score, Member, ZSet = #zset{}) ->
  case orddict:find(Member, ZSet#zset.dict) of
    error ->
      ZSet#zset{dict = orddict:store(Member, Score, ZSet#zset.dict),
                tree = gb_trees:enter({Score, Member}, undefined, ZSet#zset.tree)};
    {ok, PrevScore} ->
      ZSet#zset{dict = orddict:store(Member, Score, ZSet#zset.dict),
                tree = gb_trees:enter({Score, Member}, undefined,
                                      gb_trees:delete({PrevScore, Member}, ZSet#zset.tree))}
  end.

%% @doc Returns the size of the zset
-spec size(zset(any(), any())) -> non_neg_integer().
size(ZSet) ->
  gb_trees:size(ZSet#zset.tree).

%% @doc Returns an iterator that can be used for traversing the entries of Tree; see {@link next/1}.
-spec iterator(zset(Scores, Members)) -> iterator(Scores, Members).
iterator(ZSet) ->
  gb_trees:iterator(ZSet#zset.tree).

%% @doc Returns {Score, Member, Iter2} where Score is the smallest score referred to by the iterator
%% Iter1, and Iter2 is the new iterator to be used for traversing the remaining nodes, or the atom
%% none if no nodes remain.
 -spec next(iterator(Scores, Members)) -> none | {Scores, Members, iterator(Scores, Members)}.
next(Iter1) ->
  case gb_trees:next(Iter1) of
    none -> none;
    {{Score, Member}, _, Iter2} -> {Score, Member, Iter2}
  end.
 
%% @doc This function searches for a key in a zset. Returns {ok, Score} where Score is the score
%%      associated with Member, or error if the key is not present.
 -spec find(Member, iterator(Scores, Members)) -> Scores when is_subtype(Member, Members).
find(Member, ZSet) ->
  orddict:find(Member, ZSet#zset.dict).

%% @doc Returns the intersection of ZSet1 and ZSet2 generating the resulting scores using Aggregate
-spec intersection(fun((Scores1, Scores2) -> Scores3), zset(Scores1, Members), zset(Scores2, Members)) -> zset(Scores3, Members).
intersection(Aggregate, ZSet1, ZSet2) ->
  intersection(Aggregate, orddict:to_list(ZSet1#zset.dict), orddict:to_list(ZSet2#zset.dict), new()).

%% @doc Returns the intersection of the non-empty list of ZSets generating the resulting scores using Aggregate in order.
%%      The last argument will be the accumulated result
-spec intersection(fun((Scores, Scores) -> Scores), [zset(Scores, Members),...]) -> zset(Scores, Members).
intersection(Aggregate, [ZSet1 | ZSets]) ->
  lists:foldl(
    fun(ZSet, AccZSet) ->
            intersection(Aggregate, ZSet, AccZSet)
    end, ZSet1, ZSets).

%% @doc Executes Fun in each element and returns the zset with the scores returned by it
-spec map(fun((Scores, Members) -> Scores2), zset(Scores, Members)) -> zset(Scores2, Members).
map(Fun, ZSet) ->
  map(Fun, next(iterator(ZSet)), new()).

%% =================================================================================================
%% Private functions
%% =================================================================================================
%% @private
intersection(_Aggregate, [], _D2, Acc) -> Acc;
intersection(_Aggregate, _D1, [], Acc) -> Acc;
intersection(Aggregate, [{M, S1} | D1], [{M, S2} | D2], Acc) ->
  intersection(Aggregate, D1, D2, enter(Aggregate(S1, S2), M, Acc));
intersection(Aggregate, [{M1, _S1} | D1], [{M2, S2} | D2], Acc) when M1 < M2 ->
  intersection(Aggregate, D1, [{M2, S2} | D2], Acc);
intersection(Aggregate, [{M1, S1} | D1], [{M2, _S2} | D2], Acc) when M1 >= M2 ->
  intersection(Aggregate, [{M1, S1} | D1], D2, Acc).

%% @private
map(_Fun, none, Acc) -> Acc;
map(Fun, {Score, Member, Iter}, Acc) ->
  map(Fun, next(Iter), enter(Fun(Score, Member), Member, Acc)).