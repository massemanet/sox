-module(sox).

-include_lib("kernel/include/logger.hrl").

-export(
   [start/1,
    command/3,
    forward/2,
    info/0]).


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

info() ->
    sctp_info().

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
    Opts#{state => down, connxns => tnew()}.

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
            true = register(maps:get(name, Opts), self()),
            Opts#{socket => sctp_create(Opts), state => down}
    end.

loop(State) ->
    receive
        {'$command', From, What, Args}   -> loop(handle_command(What, Args, From, State));
        {'$send', What}                  -> loop(handle_send(What, State));
        {'$socket', Socket, select, Ref} -> loop(handle_socket(Socket, Ref, State));
        {'DOWN', _, process, Pid, I}     -> loop(handle_death(Pid, I, State))
                                                
    end.

handle_command(connect, Args, From, State)    -> do_connect(Args, From, State);
handle_command(disconnect, Args, From, State) -> do_disconnect(Args, From, State);
handle_command(listen, Args, From, State)     -> do_listen(Args, From ,State);
handle_command(unlisten, Args, From, State)   -> do_unlisten(Args, From, State);
handle_command(status, Args, From, State)     -> do_status(Args, From, State).

handle_death(Pid, Info, State) ->
    #{connxns := Connxns} = State,
    nop(death, Info, State#{connxns => tdel(Pid, Connxns)}).

handle_send(What, State) ->
    case maps:get(type, State) of
        server ->
            Connxns = maps:get(connxns, State),
            case tlen(Connxns) of
                0 -> nop(send, drop, State);
                N -> tget(rand:uniform(N), Connxns) ! What
            end;
        _ ->
           case maps:get(send_buffer_full, State, false) of
               true -> nop(send_fail, buffer_full, State);
               false -> sctp_send(What, State)
           end
    end.

handle_socket(Socket, Ref, State) ->
    case Ref of
        connect -> sctp_connect(triggered, State);
        accept -> sctp_accept_loop(Socket, State);
        send -> nop(socket, send_buffer_ok, State#{send_buffer_full => false});
        recv -> sctp_recv_loop(State);
        _ -> ?SEPPUKU(#{select_fail => Ref})
    end.
                    

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
        state -> reply(State, From, State);
        info  -> reply(sctp_info(State), From, State);
        opts  -> reply(sctp_getopts(State), From, State);
        _     -> reply(unrec, From, State)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SCTP operations against `socket'

sctp_create(State) ->
    #{local_ip := LIP, localPort := LPort} = State,
    {ok, Socket} = socket:open(inet, stream, sctp),
    ok = socket:bind(Socket, #{family => inet, addr => LIP, port => LPort}),
    State#{socket => Socket}.

sctp_recreate(State) ->
    ok = socket:close(maps:get(socket, State)),
    sctp_create(State).

sctp_listen(State) ->
    case maps:get(state, State) of
        listening -> nop(listen, already, State);
        unlistening ->
            #{socket := Socket} = State,
            ok = socket:listen(Socket),
            sctp_accept_loop(Socket, State#{state => listening})
    end.

sctp_accept_loop(Listen, State) ->
    case socket:accept(Listen, nowait) of
        {ok, Socket} -> sctp_accept_loop(Listen, create_connxn(Socket, State));
        {select, SelectInfo} -> State#{accept_ref => SelectInfo};
        {error, Reason} -> ?SEPPUKU(Reason)
    end.

create_connxn(Socket, State) ->
    #{forward := Forward, connxns := Connxns} = State,
    Opts = #{socket => Socket, type => connxn, forward => Forward},
    Connxn = start(Opts),
    State#{connxns => tput(Connxn, Connxns)}.

sctp_unlisten(State) ->
    case maps:get(state, State) of
        unlistening -> nop(unlisten, already, State);
        listening -> State#{state => unlistening, socket => sctp_recreate(State)}
    end.


sctp_connect(_Args, State) ->
    #{socket := Socket, timeout := _TO, remote_ip := RIP, remote_port := RPort} = State,
    case socket:connect(Socket, #{family => inet, addr => RIP, port => RPort}, nowait) of
        ok -> State#{state => connected};
        {select, SelectInfo} -> State#{state => connecting, connect_ref => SelectInfo};
        {error, Reason} -> ?SEPPUKU(Reason)
    end.

sctp_cancel_connect(_Args, State) ->
    case maps:get(State, connect_ref, undefined) of
        undefined -> nop(cancel_connect, already, State);
        Ref -> socket:cancel(maps:get(socket, State), Ref)
    end.

sctp_send(Payload, State) ->
    case socket:sendmsg(maps:get(socket, State), #{iov=>[Payload]}, nowait) of
        ok                        -> State;
        {ok, _}                   -> nop(send, broken_send, State);
        {select, {SelectInfo, _}} -> nop(send, broken_send, State#{send_ref => SelectInfo});
        {select, SelectInfo}      -> State#{send_ref => SelectInfo};
        {error, Reason}           -> ?SEPPUKU(#{send_error => Reason})
    end.

sctp_recv_loop(State) ->
    case socket:recvmsg(maps:get(socket, State), nowait) of
        {ok, Msg}            -> do_forward(Msg, State);
        {select, SelectInfo} -> State#{recv_ref => SelectInfo};
        {error, Reason}      -> ?SEPPUKU(#{recv_fail => Reason})
    end.

do_forward(Msg, State) ->
    case maps:get(forward, State, undefined) of
        undefined -> nop(forward, drop, State);
        Dest ->
            Dest ! Msg,
            State
    end.
             
sctp_shutdown(_Args, State) ->
    case socket:shutdown(maps:get(socket, State), write) of
        ok -> State#{state => closing};
        {error, Reason} -> ?SEPPUKU(#{shutdown_error => Reason})
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

where(M, F, L) ->
    atom_to_list(M)++":"++atom_to_list(F)++":"++integer_to_list(L).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% use a tuple as fast array

tnew() ->
    {0}.

tlen(T) ->
    element(1, T).

tget(N, T) ->
    element(N+1, T).

tput(V, T) ->
    [N|L] = tuple_to_list(T),
    list_to_tuple([N+1, V|L]).

tdel(V, T) ->
    [N|L] = tuple_to_list(T),
    case L--[V] of
        L -> T;
        O -> list_to_tuple([N-1|O])
    end.
