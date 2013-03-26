%%%-------------------------------------------------------------------
%%% File    : edis_node_monitor.erl
%%% Author  : Joachim Nilsson <joachim@inakanetworks.com>
%%% Description : A node monitor for multi-node support
%%%
%%% Created :  22 March 2013
%%%-------------------------------------------------------------------
-module(edis_node_monitor).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).
-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

multi_node_set(Key,Value) -> gen_server:abcast('bar@Joachims-MacBook-Air.local', 'bar', {set, Key, Value}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
  ok = pg2:join(node_group, self()),
  ok = net_kernel:monitor_nodes(true, [nodedown_reason]),
  net_adm:world(),
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->




  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({Event, Node, Data}, State) ->


%% INFO ABOUT NODES SHOULD COME IN HERE. CHECK OUT FORMAT (monitor_nodes?) AND CATCH
%% Is this the format that monitor_nodes will deliver a message? {Event, Node, Data}

  case Event of
     nodeup ->
       lager:notice("~n~n\t\t~p is UP! (~p)~n~n", [Node, Data]);
     nodedown ->
       lager:notice("~n~n\t\t~p is DOWN! (~p).", [Node, Data]);
     nodegone ->
      lager:notice("~n~n\t\t~p is GONE! (~p).", [Node, Data])
  end,


  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------