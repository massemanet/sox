-module(sox).

-include_lib("kernel/include/logger.hrl").

-export(
   [start/1,
    command/3,
    forward/2]).


-define(SEPPUKU(X), error(#{where => where(?MODULE, ?FUNCTION_NAME, ?LINE), why => X})).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This module implements three kinds of socket owning processes; a server, a
%% client, and a connxn. A server has a listen socket and a list of connxns
%% (i.e. pids). A client has a connected/connecting socket. A connxn has a
%% connected socket (which we get from `accept').

start(Opts) ->
    spawn(init(validate(Opts))).

command(Instance, What, Args) ->
    Instance ! {'$command', What, Args}.

forward(Instance, What) ->
    Instance ! {'$forward', What}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%

validate(Opts) ->
        case Opts of
            #{type := server} -> validate_server(Opts);
            #{type := client} -> validate_client(Opts);
            #{type := connxn} -> validate_connxn(Opts)
        end.

validate_server(Opts) ->
    #{name := _Name, local_ip := _IP, local_port := _Port} = Opts,
    Opts#{state => down, connxns => []}.

validate_client(Opts) ->
    #{name := _Name, local_ip := _LIP, local_port := _LPort,
      remote_ip := _RIP, remote_port := _RPort, timeout := _TO} = Opts,
    Opts#{state => closed, timer => undefined}.

validate_connxn(Opts) ->
    #{socket := _Socket} = Opts,
    Opts.

init(Opts) ->
    loop(setup(Opts)).

setup(Opts) ->
    case maps:get(type, Opts) of
        connxn -> Opts;
        _ ->
            #{name := Name, local_ip := IP, local_port := Port} = Opts,
            true = register(Name, self()),
            {ok, S} = socket:open(inet, stream, sctp),
            ok = socket:bind(S, #{family => inet, addr => IP, port => Port}),
            Opts#{socket => S, state => down}
    end.

loop(State) ->
    receive
        {'$command', From, What, Args}   -> loop(handle_command(What, Args, From, State));
        {'$forward', What}               -> loop(handle_forward(What, State));
        {'$socket', Socket, select, Ref} -> loop(handle_select(Socket, Ref, State))
    end.

handle_command(connect, Args, From, State)    -> do_connect(Args, From, State);
handle_command(disconnect, Args, From, State) -> do_disconnect(Args, From, State);
handle_command(listen, Args, From, State)     -> do_listen(Args, From ,State);
handle_command(unlisten, Args, From, State)   -> do_unlisten(Args, From, State);
handle_command(status, Args, From, State)     -> do_status(Args, From, State).

do_connect(Args, From, State) ->
    case maps:get(type, State) of
        client ->
            case maps:get(state, State) of
                closed -> reply(ok, From, sctp_connect(Args, State));
                closing -> reply(fail, From, nop(State));
                connecting -> reply(already, From, nop(State));
                connected -> reply(already, From, nop(State))
            end;
        _ ->
            reply(ok, From, nop(State))
    end.

do_disconnect(Args, From, State) ->
    case maps:get(type, State) of
        server -> disconnect_server(Args, From, State);
        client -> disconnect_client(Args, From, State);
        connxn -> reply(ok, From, sctp_shutdown(Args, State))
    end.

disconnect_server(Args, From, State) ->
    #{connxns := Connxns} = State,
    lists:foreach(fun(C) -> sox:command(C, shutdown, Args) end, Connxns),
    reply(ok, From, State).

disconnect_client(Args, From, State) ->
    case maps:get(state, State) of
        closed -> reply(already, From, nop(disconnect_client, Args, State));
        closing -> reply(already, From, nop(State));
        connecting -> reply(ok, From, sctp_cancel_connect(Args, State));
        connected -> reply(ok, From, sctp_shutdown(Args, State))
    end.

do_listen(Args, From, State) ->
    case maps:get(type, State) of
        server -> reply(ok, From, sctp_listen(State));
        _ -> reply(fail, From, nop(listen, Args, State))
    end.

do_unlisten(Args, From, State) ->
    case maps:get(type, State) of
        server -> reply(ok, From, sctp_unlisten(State));
        _ -> reply(fail, From, nop(unlisten, Args, State))
    end.

do_status(Args, From, State) ->
    case Args of
        state ->
            reply(State, From, State);
        opts ->
            reply(getopts(maps:get(socket, State)), From, State)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SCTP operations against `socket'

sctp_listen(State) ->
    case maps:get(state, State) of
        listening -> nop(listen, already_listening, State);
        unlistening ->
            #{socket := Socket} = State,
            ok = socket:listen(Socket),
            accept_loop(Socket, State)
    end.

accept_loop(Listen, State) ->
    case socket:accept(Listen, nowait) of
        {ok, Connxn} -> accept_loop(Listen, update_val(connxns, {prepend, Connxn}, State));
        {select, SelectInfo} -> State#{accept_ref => SelectInfo};
        {error, Reason} -> ?SEPPUKU(Reason)
    end.

sctp_connect(State) ->
    #{socket := Socket, timeout := _TO, remote_ip := RIP, remote_port := RPort} = State,
    case socket:connect(Socket, #{family => inet, addr => RIP, port => RPort}, nowait) of
        ok -> State#{state => connected};
        {select, SelectInfo} -> State#{state => connecting, connect_ref => SelectInfo};
        {error, Reason} -> ?SEPPUKU(Reason)
    end.

sctp_send(Payload, State) ->
    case socket:sendmsg(Socket, #{iov=>[Payload]}, nowait) of
        ok -> ;
        {ok, RestData} -> ;
        {select, SelectInfo} -> ;
        {select, {SelectInfo, RestData}} -> ;
        {error, Reason} -> ;
        {error, {Reason, RestData}} ->
    end.


sctp_recv(S) ->
    case socket:recvmsg(S, nowait) of
        {ok,#{addr := #{addr := Addr,port := Port}, iov := [Msg]}} ->
            {ok, #{from => {Addr, Port}, msg => Msg}};
        {select, SelectInfo} -> ;
        {select,{select_info, recvmsg, Handle}} -> ;
        {error, Reason} ->
    end.

sctp_shutdown(State) ->
    case socket:shutdown(Socket, write) of
        ok -> State#{state => closing};
        {error, Reason} ->
    end.


sctp_info() ->
    lists:map(fun sctp_info/1, socket:which_sockets(sctp)).

sctp_info(State) ->
    socket:info(maps:get(socket, State)).

sctp_getopts(State) ->
    lists:foldl(mk_sctp_getopt(State), [], opts()).

mk_sctp_getopt(#{socket := Socket}) ->
    fun(Opt, Acc) -> sctp_getopt(Socket, Opt, Acc) end.

sctp_getopt(Socket, Opt, Acc) ->
    case socket:getopt(Socket, Opt) of
        {ok, Val} -> [{Opt, Val}|Acc];
        {error, _} -> Acc
    end.

opts() ->
    lists:append(
      [[{socket, O}
        || O <- [acceptconn, acceptfilter, bindtodevice, broadcast,
                 busy_poll, debug, domain, dontroute, error,
                 keepalive, linger, mark, oobinline, passcred,
                 peek_off, peercred, priority, protocol, rcvbuf,
                 rcvbufforce, rcvlowat, rcvtimeo, reuseaddr,
                 reuseport, rxq_ovfl, setfib, sndbuf, sndbufforce,
                 sndlowat, sndtimeo, timestamp, type]],
       [{ip, O}
        || O <- [add_membership, add_source_membership, block_source,
                 dontfrag, drop_membership, drop_source_membership,
                 freebind, hdrincl, minttl, msfilter, mtu,
                 mtu_discover, multicast_all, multicast_if,
                 multicast_loop, multicast_ttl, nodefrag, options,
                 pktinfo, recvdstaddr, recverr, recvif, recvopts,
                 recvorigdstaddr, recvtos, recvttl, retopts,
                 router_alert, sndsrcaddr, tos, transparent, ttl,
                 unblock_source]],
       [{sctp, O}
        || O <- [adaption_layer, associnfo, auth_active_key,
                 auth_asconf, auth_chunk, auth_key, auth_delete_key,
                 autoclose, context, default_send_params,
                 delayed_ack_time, disable_fragments, hmac_ident,
                 events, explicit_eor, fragment_interleave,
                 get_peer_addr_info, initmsg, i_want_mapped_v4_addr,
                 local_auth_chunks, maxseg, maxburst, nodelay,
                 partial_delivery_point, peer_addr_params,
                 peer_auth_chunks, primary_addr, reset_streams,
                 rtoinfo, set_peer_primary_addr, status,
                 use_ext_recvinfo]]]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utilities

reply(Reply, From, State) ->
    From ! Reply,
    State.

nop(State) ->
    State.

nop(Action, Args, State) ->
    ?LOG_INFO(#{action => Action, args => Args, state => State}),
    State.

update_val(Key, {prepend, Val}, Map) ->
    Map#{Key => [Val|maps:get(Key, Map)]}.

where(M, F, L) ->
    atom_to_list(M)++":"++atom_to_list(F)++":"++integer_to_list(L).
