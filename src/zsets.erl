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
               tree :: edis_gb_trees:edis_gb_tree()}).
-opaque zset(_Scores, _Members) :: #zset{}.

-type limit(Scores) :: neg_infinity | infinity | {exc, Scores} | {inc, Scores}.
-type aggregate() :: sum | max | min.
-export_type([limit/1, aggregate/0]).

-type direction() :: forward | backwards.
-record(zset_iterator, {direction = forward :: direction(),
                        iterator            :: edis_gb_tree:iter()}).

-opaque iterator(_Scores, _Members) :: #zset_iterator{}.
-export_type([zset/2, iterator/2]).

-export([new/0, enter/2, enter/3, size/1, find/2, delete_any/2]).
-export([iterator/1, iterator/2, direction/1, next/1, map/2, to_list/1]).
-export([intersection/2, intersection/3, union/2, union/3]).
-export([count/3, range/3, range/4, list/3, list/4]).

%% @doc Creates an empty {@link zset(any(), any())}
-spec new() -> zset(any(), any()).
new() ->
  #zset{dict = dict:new(), tree = edis_gb_trees:empty()}.

%% @equiv enter(Score, Member, ZSet)
-spec enter({Score, Member}, zset(Scores, Members)) -> zset(Scores, Members) when is_subtype(Score, Scores), is_subtype(Member, Members).
enter({Score, Member}, ZSet) ->
  enter(Score, Member, ZSet).

%% @doc Adds a new element to the zset.
%%      If the element is already present it justs updates its score
-spec enter(Score, Member, zset(Scores, Members)) -> zset(Scores, Members) when is_subtype(Score, Scores), is_subtype(Member, Members).
enter(Score, Member, ZSet = #zset{}) ->
  case dict:find(Member, ZSet#zset.dict) of
    error ->
      ZSet#zset{dict = dict:store(Member, Score, ZSet#zset.dict),
                tree = edis_gb_trees:enter({Score, Member}, undefined, ZSet#zset.tree)};
    {ok, PrevScore} ->
      ZSet#zset{dict = dict:store(Member, Score, ZSet#zset.dict),
                tree = edis_gb_trees:enter({Score, Member}, undefined,
                                           edis_gb_trees:delete({PrevScore, Member},
                                                                ZSet#zset.tree))}
  end.

%% @doc Removes the node with key Key from Tree1 if the key is present in the tree, otherwise does 
%%      nothing; returns new tree.
-spec delete_any(Members, zset(Scores, Members)) -> zset(Scores, Members).
delete_any(Member, ZSet) ->
  case dict:find(Member, ZSet#zset.dict) of
    error -> ZSet;
    {ok, Score} ->
      ZSet#zset{dict = dict:erase(Member, ZSet#zset.dict),
                tree = edis_gb_trees:delete_any({Score, Member}, ZSet#zset.tree)}
  end.

%% @doc Returns the size of the zset
-spec size(zset(any(), any())) -> non_neg_integer().
size(ZSet) ->
  dict:size(ZSet#zset.dict).

%% @equiv iterator(ZSet, forward).
-spec iterator(zset(Scores, Members)) -> iterator(Scores, Members).
iterator(ZSet) ->
  iterator(ZSet, forward).

%% @doc Returns an iterator that can be used for traversing the entries of Tree; see {@link next/1}.
-spec iterator(zset(Scores, Members), direction()) -> iterator(Scores, Members).
iterator(ZSet, forward) ->
  #zset_iterator{direction = forward,
                 iterator = edis_gb_trees:iterator(ZSet#zset.tree)};
iterator(ZSet, backwards) ->
  #zset_iterator{direction = backwards,
                 iterator = edis_gb_trees:rev_iterator(ZSet#zset.tree)}.

%% @doc Returns {Score, Member, Iter2} where Score is the smallest score referred to by the iterator
%% Iter1, and Iter2 is the new iterator to be used for traversing the remaining nodes, or the atom
%% none if no nodes remain.
 -spec next(iterator(Scores, Members)) -> none | {Scores, Members, iterator(Scores, Members)}.
next(Iter1) ->
  Function =
    case Iter1#zset_iterator.direction of
      forward -> next;
      backwards -> previous
    end,
  case edis_gb_trees:Function(Iter1#zset_iterator.iterator) of
    none -> none;
    {{Score, Member}, _, Iter2} -> {Score, Member, Iter1#zset_iterator{iterator = Iter2}}
  end.

%% @doc Returns the direction of the iterator
-spec direction(iterator(_Scores, _Members)) -> direction().
direction(Iter) -> Iter#zset_iterator.direction.

%% @doc This function searches for a key in a zset. Returns {ok, Score} where Score is the score
%%      associated with Member, or error if the key is not present.
 -spec find(Member, iterator(Scores, Members)) -> Scores when is_subtype(Member, Members).
find(Member, ZSet) ->
  dict:find(Member, ZSet#zset.dict).

%% @doc Returns the intersection of ZSet1 and ZSet2 generating the resulting scores using Aggregate
-spec intersection(fun((Scores1, Scores2) -> Scores3), zset(Scores1, Members), zset(Scores2, Members)) -> zset(Scores3, Members).
intersection(Aggregate, ZSet1, ZSet2) ->
  intersection(Aggregate, lists:sort(dict:to_list(ZSet1#zset.dict)),
               lists:sort(dict:to_list(ZSet2#zset.dict)), new()).

%% @doc Returns the intersection of the non-empty list of ZSets generating the resulting scores using Aggregate in order.
%%      The last argument will be the accumulated result
-spec intersection(fun((Scores, Scores) -> Scores), [zset(Scores, Members),...]) -> zset(Scores, Members).
intersection(Aggregate, [ZSet1 | ZSets]) ->
  lists:foldl(
    fun(ZSet, AccZSet) ->
            intersection(Aggregate, ZSet, AccZSet)
    end, ZSet1, ZSets).

%% @doc Returns the union of ZSet1 and ZSet2 generating the resulting scores using Aggregate
-spec union(fun((Scores1, Scores2) -> Scores3), zset(Scores1, Members), zset(Scores2, Members)) -> zset(Scores3, Members).
union(Aggregate, ZSet1, ZSet2) ->
  union(Aggregate, dict:to_list(ZSet1#zset.dict), dict:to_list(ZSet2#zset.dict), new()).

%% @doc Returns the union of the non-empty list of ZSets generating the resulting scores using Aggregate in order.
%%      The last argument will be the accumulated result
-spec union(fun((undefined|Scores, undefined|Scores) -> Scores), [zset(Scores, Members),...]) -> zset(Scores, Members).
union(Aggregate, [ZSet1 | ZSets]) ->
  lists:foldl(
    fun(ZSet, AccZSet) ->
            union(Aggregate, ZSet, AccZSet)
    end, ZSet1, ZSets).

%% @doc Executes Fun in each element and returns the zset with the scores returned by it
-spec map(fun((Scores, Members) -> Scores2), zset(Scores, Members)) -> zset(Scores2, Members).
map(Fun, ZSet) ->
  map(Fun, next(iterator(ZSet)), new()).

%% @doc Converts the sorted set into a list of {Score, Member} pairs
-spec to_list(zset(Scores, Members)) -> [{Scores, Members}].
to_list(ZSet) ->
  edis_gb_trees:keys(ZSet#zset.tree).

%% @doc Returns the number of elements between Min and Max in ZSet
-spec count(limit(Scores), limit(Scores), zset(Scores, _Members)) -> non_neg_integer().
count(Min, Max, ZSet) ->
  count(Min, Max, next(iterator(ZSet)), 0).

%% @equiv range(Start, Stop, ZSet, forward)
-spec range(non_neg_integer(), non_neg_integer(), zset(Scores, Members)) -> [{Scores, Members}].
range(Start, Stop, ZSet) ->
  range(Start, Stop, ZSet, forward).

%% @doc Returns the list of elements between the Start'th one and the Stop'th one inclusive
-spec range(non_neg_integer(), non_neg_integer(), zset(Scores, Members), direction()) -> [{Scores, Members}].
range(Start, Stop, ZSet, Direction) ->
  lists:reverse(range(Start, Stop, next(iterator(ZSet, Direction)), 1, [])).

%% @equiv list(Min, Max, ZSet, forward)
-spec list(limit(Scores), limit(Scores), zset(Scores, Members)) -> [{Scores, Members}].
list(Min, Max, ZSet) ->
  list(Min, Max, ZSet, forward).

%% @doc Returns the list of elements with scores between the specified limits
-spec list(limit(Scores), limit(Scores), zset(Scores, Members), direction()) -> [{Scores, Members}].
list(Min, Max, ZSet, Direction) ->
  lists:reverse(do_list(Min, Max, next(iterator(ZSet, Direction)), [])).

%% =================================================================================================
%% Private functions
%% =================================================================================================
%% @private
do_list(_Min, _Max, none, Acc) -> Acc;
do_list(Min, Max, {Score, Member, Iterator}, Acc) ->
  case {check_limit(min, Min, Score, Iterator#zset_iterator.direction),
        check_limit(max, Max, Score, Iterator#zset_iterator.direction)} of
    {in, in} -> do_list(Min, Max, next(Iterator), [{Score, Member}|Acc]);
    {_, out} -> Acc;
    {out, in} -> do_list(Min, Max, next(Iterator), Acc)
  end.

%% @private
count(_Min, _Max, none, Acc) -> Acc;
count(Min, Max, {Score, _Member, Iterator}, Acc) ->
  case {check_limit(min, Min, Score, Iterator#zset_iterator.direction),
        check_limit(max, Max, Score, Iterator#zset_iterator.direction)} of
    {in, in} -> count(Min, Max, next(Iterator), Acc + 1);
    {_, out} -> Acc;
    {out, in} -> count(Min, Max, next(Iterator), Acc)
  end.

%% @private
range(_Start, _Stop, none, _, Acc) -> Acc;
range(Start, Stop, {_Score, _Member, Iter}, Pos, Acc) when Pos < Start ->
  range(Start, Stop, next(Iter), Pos+1, Acc);
range(Start, Stop, {Score, Member, Iter}, Pos, Acc) when Pos =< Stop ->
  range(Start, Stop, next(Iter), Pos+1, [{Score, Member} | Acc]);
range(_, _, _, _, Acc) -> Acc.

%% @private
check_limit(min, neg_infinity, _, forward) -> in;
check_limit(min, infinity, _, forward) -> out;
check_limit(min, {exc, Min}, Score, forward) when Min < Score -> in;
check_limit(min, {exc, Min}, Score, forward) when Min >= Score -> out;
check_limit(min, {inc, Min}, Score, forward) when Min =< Score -> in;
check_limit(min, {inc, Min}, Score, forward) when Min > Score -> out;
check_limit(max, neg_infinity, _, forward) -> out;
check_limit(max, infinity, _, forward) -> in;
check_limit(max, {exc, Max}, Score, forward) when Max > Score -> in;
check_limit(max, {exc, Max}, Score, forward) when Max =< Score -> out;
check_limit(max, {inc, Max}, Score, forward) when Max >= Score -> in;
check_limit(max, {inc, Max}, Score, forward) when Max < Score -> out;
check_limit(min, neg_infinity, _, backwards) -> out;
check_limit(min, infinity, _, backwards) -> in;
check_limit(min, {exc, Min}, Score, backwards) when Min > Score -> in;
check_limit(min, {exc, Min}, Score, backwards) when Min =< Score -> out;
check_limit(min, {inc, Min}, Score, backwards) when Min >= Score -> in;
check_limit(min, {inc, Min}, Score, backwards) when Min < Score -> out;
check_limit(max, neg_infinity, _, backwards) -> in;
check_limit(max, infinity, _, backwards) -> out;
check_limit(max, {exc, Max}, Score, backwards) when Max < Score -> in;
check_limit(max, {exc, Max}, Score, backwards) when Max >= Score -> out;
check_limit(max, {inc, Max}, Score, backwards) when Max =< Score -> in;
check_limit(max, {inc, Max}, Score, backwards) when Max > Score -> out.

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
union(_Aggregate, [], [], Acc) -> Acc;
union(Aggregate, [{M, S1} | D1], [], Acc) ->
  union(Aggregate, D1, [], enter(Aggregate(S1, undefined), M, Acc));
union(Aggregate, [], [{M, S2} | D2], Acc) ->
  union(Aggregate, [], D2, enter(Aggregate(undefined, S2), M, Acc));
union(Aggregate, [{M, S1} | D1], [{M, S2} | D2], Acc) ->
  union(Aggregate, D1, D2, enter(Aggregate(S1, S2), M, Acc));
union(Aggregate, [{M1, S1} | D1], [{M2, S2} | D2], Acc) when M1 < M2 ->
  union(Aggregate, D1, [{M2, S2} | D2], enter(Aggregate(S1, undefined), M1, Acc));
union(Aggregate, [{M1, S1} | D1], [{M2, S2} | D2], Acc) when M1 >= M2 ->
  union(Aggregate, [{M1, S1} | D1], D2, enter(Aggregate(undefined, S2), M2, Acc)).

%% @private
map(_Fun, none, Acc) -> Acc;
map(Fun, {Score, Member, Iter}, Acc) ->
  map(Fun, next(Iter), enter(Fun(Score, Member), Member, Acc)).