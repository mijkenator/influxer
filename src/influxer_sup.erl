%%%-------------------------------------------------------------------
%% @doc influxer top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(influxer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
	_BatcherSup = {batcher_sup, {batcher_sup, start_link, []}, permanent, 5000, supervisor, [batcher_sup]},
    {ok, { {one_for_all, 10, 10}, [_BatcherSup]} }.

%%====================================================================
%% Internal functions
%%====================================================================
