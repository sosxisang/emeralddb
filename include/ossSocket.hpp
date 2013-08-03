/*******************************************************************************
   Copyright (C) 2013 SequoiaDB Software Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.
*******************************************************************************/
#ifndef OSSNETWORK_HPP_
#define OSSNETWORK_HPP_

#include "core.hpp"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <string.h>
#define SOCKET_GETLASTERROR errno

// by default 10ms timeout
#define OSS_SOCKET_DFT_TIMEOUT 10000

// max hostname
#define OSS_MAX_HOSTNAME NI_MAXHOST
#define OSS_MAX_SERVICENAME NI_MAXSERV

class _ossSocket
{
private :
   int _fd ;
   socklen_t _addressLen ;
   socklen_t _peerAddressLen ;
   struct sockaddr_in  _sockAddress ;
   struct sockaddr_in  _peerAddress ;

   bool _init ;
   int _timeout ;
protected:
   unsigned int _getPort ( sockaddr_in *addr ) ;
   int _getAddress ( sockaddr_in *addr, char *pAddress, unsigned int length ) ;

public :
   int setSocketLi ( int lOnOff, int linger ) ;
   void setAddress(const char * pHostName, unsigned int port );
   // Create a listening socket
   _ossSocket();
   _ossSocket ( unsigned int port, int timeout = 0 ) ;
   // Create a connecting socket
   _ossSocket ( const char *pHostname, unsigned int port, int timeout = 0 ) ;
   // Create from a existing socket
   _ossSocket ( int *sock, int timeout = 0 ) ;
   ~_ossSocket ()
   {
      close () ;
   }
   int initSocket () ;
   int bind_listen () ;
   bool isConnected () ;
   int send ( const char *pMsg, int len,
                int timeout = OSS_SOCKET_DFT_TIMEOUT,
                int flags = 0 ) ;
   int recv ( char *pMsg, int len,
                int timeout = OSS_SOCKET_DFT_TIMEOUT,
                int flags = 0 ) ;
   int recvNF ( char *pMsg, int &len,
                  int timeout = OSS_SOCKET_DFT_TIMEOUT ) ;

   int connect () ;
   void close () ;
   int accept ( int *sock, struct sockaddr *addr, socklen_t *addrlen,
                  int timeout = OSS_SOCKET_DFT_TIMEOUT ) ;
   int disableNagle () ;

   unsigned int getPeerPort () ;
   int getPeerAddress ( char *pAddress, unsigned int length ) ;

   unsigned int getLocalPort () ;
   int getLocalAddress ( char *pAddress, unsigned int length ) ;

   int setTimeout ( int seconds ) ;

   static int getHostName ( char *pName, int nameLen ) ;
   static int getPort ( const char *pServiceName, unsigned short &port ) ;
};

typedef class _ossSocket ossSocket ;

#endif
