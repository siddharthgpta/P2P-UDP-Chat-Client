from collections import deque
import time
import socket
import sys
import select
import enum
import re
import random
import copy

class Attribute(enum.IntEnum):
    __order__ = "Source Destination ProtocolNumber HopCount MessageIDNumber ViewLog Message"

    Source = 0
    Destination = 1
    ProtocolNumber = 2
    HopCount = 3
    MessageIDNumber = 4
    ViewLog = 5
    Message = 6

FieldName = [
    "SRC",
    "DST",
    "PNUM",
    "HCT",
    "MNUM",
    "VL",
    "MESG"
]

class Protocol(enum.IntEnum):
    ErrorMessage = 0
    RegistrationRequest = 1
    RegistrationResponse = 2
    Data = 3
    DataConfirmation = 4
    RequestPeerRegistry = 5
    CurrentPeerRegistry = 6
    SerialBroadcast = 7
    SerialBroadcastConfirmation = 8

class MessageType(enum.IntEnum):
    Direct = 0
    Broadcast = 1
    Forward = 2

def AppendTag(attributeType, attributeValue):
    return FieldName[attributeType] + ":" + attributeValue + ";"

def FormatMessage(attributeValues):
    message = ""
    for tag, value in zip(Attribute, attributeValues):
        message += AppendTag(tag, value)

    return message[:-1]

def UpdatePeerRegistry(listOfPeers):
    dictOfPeers = {}
    for item in listOfPeers:
        info = item.split("=", 1)
        peerID = info[0]
        details = info[1].split("@", 1)
        dictOfPeers[peerID] = (details[0], int(details[1]))
    
    return dictOfPeers

def DisplayIDS(peerList, recentlySeenPeers):
    peerList.pop('999', None) 
    print("******************************")
    print("Recently Seen Peers:")
    for peer in recentlySeenPeers:
        sys.stdout.write(peer + ", ")
    sys.stdout.write("\b\b \n")
    sys.stdout.flush()
    print("Known addresses:")
    for peer in peerList:
        print(peer + "\t" + peerList[peer][0] + "\t" + str(peerList[peer][1]))
    print("******************************")

class Message:
    def __init__(self, messageString = ""):
        self.parsedMessage = dict(item.split(":", 1) for item in messageString.split(";", 7))
        self.intendedRecepients = []
        self.retryCount = 1
        self.timer = 0
        self.messageType = None

    def Format(self, peerID):
        message = ""
        if self.messageType != MessageType.Forward:
            self.parsedMessage[FieldName[Attribute.Destination]] = peerID
        for attrKey in FieldName:
            message += attrKey + ":" + self.parsedMessage[attrKey] + ";"

        return message[:-1]

    def GetTagValue(self, attributeType):
        try:
            return self.parsedMessage[FieldName[attributeType]]
        except:
            return None

class ConnectionHandler:
    peerList = {}
    unknownPeers = {}
    recentlySeenPeers = {}

    def __init__(self, serverDetails):
        self.clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client = ("", 0)
        self.clientID = None
        self.messageID = 100
        self.ackMessage = []
        self.peerList['999'] = serverDetails #server
        self.localInput = deque()
        self.networkInput = deque()
        self.networkOutput = deque()

    def __del__(self):
        self.clientSocket.close()
        #print("Connection closed.")

    def RequestPeerList(self):
        idsValues = [self.clientID, "999", str(Protocol.RequestPeerRegistry.value), "1", str(self.messageID), "", "get map"]
        requestMessage = Message(FormatMessage(idsValues))
        requestMessage.intendedRecepients = ['999']
        requestMessage.timer = time.time()
        self.networkOutput.append(requestMessage)
        self.ackMessage.append(requestMessage)
        self.messageID += 1

    def AcknowledgeMsg(self, nwInput):
        pnum = int(nwInput.GetTagValue(Attribute.ProtocolNumber))
        nwInput.parsedMessage[FieldName[Attribute.Source]], nwInput.parsedMessage[FieldName[Attribute.Destination]] = nwInput.parsedMessage[FieldName[Attribute.Destination]], nwInput.parsedMessage[FieldName[Attribute.Source]]
        nwInput.parsedMessage[FieldName[Attribute.Message]] = "ACK"
        nwInput.parsedMessage[FieldName[Attribute.ProtocolNumber]] = str(pnum + 1)
        nwInput.parsedMessage[FieldName[Attribute.HopCount]] = "1"
        nwInput.intendedRecepients = [nwInput.parsedMessage[FieldName[Attribute.Destination]]]
        self.networkOutput.append(nwInput)

    def SendData(self, userInput):
        msg = ""
        pnum = 7 #default broadcast to all (known) peers
        peerID = ""
        hct = 1
        peers = self.peerList.keys()
        peers.remove("999")
        VL = ""
        messagetype = None

        regex = re.compile("^(msg (.*))$")
        match = regex.match(userInput)
        if match:
            regex = re.compile("^(msg [0-9]{3} (.*))$")
            match = regex.match(userInput)
            if not match:
                print("Peer ID format is invalid. Please try again.")
                return

            peerID = userInput[4:7]
            if peerID not in peers:
                hct = 9
                if len(peers) > 3:
                    random.shuffle(peers)
                    peers = peers[0:3]
                VL = self.clientID
                messagetype = MessageType.Forward
            else:
                peers = [peerID]
                peerID = ""
                messagetype = MessageType.Direct

            msg = userInput[match.end():].strip()
            pnum = 3
        else:
            msg = userInput[3:].strip()
            peerID = ""
            messagetype = MessageType.Broadcast

        msg = msg.translate(None, ''.join(["\'", "\"", ";", ":"])) #remove all invalid characters from the input
        msg = msg[:200] #truncate the input string to 200 characters
        
        dataParams = [self.clientID, peerID, str(pnum), str(hct), str(self.messageID), VL, msg]
        dataMessage = Message(FormatMessage(dataParams))
        dataMessage.intendedRecepients = peers
        dataMessage.messageType = messagetype
        dataMessage.timer = time.time()
        
        self.networkOutput.append(dataMessage)
        self.ackMessage.append(dataMessage)
        self.messageID += 1

    def ProcessNetworkInput(self, nwInput):
        pnum = int(nwInput.GetTagValue(Attribute.ProtocolNumber))

        if pnum in [Protocol.DataConfirmation, Protocol.SerialBroadcastConfirmation, Protocol.RegistrationResponse, Protocol.CurrentPeerRegistry]:
            for x in self.ackMessage:
                if x.messageType == MessageType.Forward: 
                    continue
                source = nwInput.GetTagValue(Attribute.Source)
                if source not in x.intendedRecepients or x.GetTagValue(Attribute.MessageIDNumber) != nwInput.GetTagValue(Attribute.MessageIDNumber): 
                    continue
                x.intendedRecepients.remove(source)
                if not x.intendedRecepients: 
                    self.ackMessage.remove(x)

        if pnum in [Protocol.Data, Protocol.SerialBroadcast]:
            print(nwInput.GetTagValue(Attribute.Source) + " sent: " + nwInput.GetTagValue(Attribute.Message))
            self.AcknowledgeMsg(nwInput)
        
        if Protocol.CurrentPeerRegistry == pnum:
            message = nwInput.GetTagValue(Attribute.Message).split("and", 1)
            self.recentlySeenPeers = message[0].split("=", 1)[1].split(",")
            server = self.peerList['999']
            self.peerList = UpdatePeerRegistry(message[1].split(","))
            self.peerList['999'] = server
            self.peerList.pop(self.clientID, None) # delete your own details
            DisplayIDS(dict(self.peerList), self.recentlySeenPeers)
        
        if Protocol.RegistrationResponse == pnum:
            self.clientID = nwInput.GetTagValue(Attribute.Destination)
            print("Successfully registered. My ID is: " + self.clientID)
        
        if Protocol.ErrorMessage == pnum:
            print("Error occured! Received: " + parsedResponse.GetTagValue(Attribute.Messages))

    def ProcessForwards(self, nwInput):
        nwInput.messageType = MessageType.Forward
        pnum = int(nwInput.GetTagValue(Attribute.ProtocolNumber))
        if pnum == Protocol.DataConfirmation:
            for x in self.ackMessage:
                source = nwInput.GetTagValue(Attribute.ViewLog)
                if source not in x.intendedRecepients or x.GetTagValue(Attribute.MessageIDNumber) != nwInput.GetTagValue(Attribute.MessageIDNumber): 
                    continue
                x.intendedRecepients.remove(source)
                if not x.intendedRecepients: 
                    self.ackMessage.remove(x)
            return
        
        ackMsg = copy.deepcopy(nwInput)
        self.AcknowledgeMsg(ackMsg)

        src = nwInput.GetTagValue(Attribute.Source)
        dest = nwInput.GetTagValue(Attribute.Destination)
        msg = nwInput.GetTagValue(Attribute.Message)
        print("Received from " + src + " to " + dest + " - " + msg)

        hct = int(nwInput.GetTagValue(Attribute.HopCount))

        if hct < 1:
            print("********************")
            print("Dropped message from " + src + " to " + dest + " - hop count exceeded")
            print("MESG: " + msg)
            print("********************")
            return

        VL = nwInput.GetTagValue(Attribute.ViewLog).split(",")

        if self.clientID in VL:
            print("********************")
            print("Dropped message from " + src + " to " + dest + " - peer revisited")
            print("MESG: " + msg)
            print("********************")
            return

        hct -= 1
        VL.append(self.clientID)
        nwInput.parsedMessage[FieldName[Attribute.ViewLog]] = ",".join(VL)
        nwInput.parsedMessage[FieldName[Attribute.HopCount]] = str(hct)

        peers = self.peerList.keys()
        peers.remove("999")
        peers = [x for x in peers if x not in VL]

        if nwInput.GetTagValue(Attribute.Destination) in peers:
            nwInput.intendedRecepients = [dest]
            nwInput.timer = time.time()
            self.networkOutput.append(nwInput)
            self.ackMessage.append(nwInput)
            return

        if len(peers) > 3:
            random.shuffle(peers)
            peers = peers[0:3]
        
        nwInput.intendedRecepients = peers
        nwInput.timer = time.time()
        self.networkOutput.append(nwInput)
        self.ackMessage.append(nwInput)

    def HandleIO(self):
        """Handle receiving udp packets"""
        try:
            nwInput = self.networkInput.popleft()
            if (nwInput.messageType != MessageType.Forward and nwInput.GetTagValue(Attribute.Destination) == self.clientID) or int(nwInput.GetTagValue(Attribute.ProtocolNumber)) == Protocol.RegistrationResponse:
                self.ProcessNetworkInput(nwInput)
            
            else:
                self.ProcessForwards(nwInput)
        
        except IndexError:
            pass
        
        except Exception as e:
            print("Unhandled exception: %s" % e)
            raise

        """Handle receiving local input"""
        try:
            userInput = self.localInput.popleft()
            if userInput[:3].lower() in ["msg", "all"]:
                self.SendData(userInput)
            
            elif "ids" == userInput.lower():
                self.RequestPeerList()
            
            else:
                print("Please enter a valid input!")

        except IndexError:
            pass
        
        except Exception as e:
            print("Unhandled exception: %s" % e)
            raise

        """Handle retry"""
        for x in self.ackMessage:
            if time.time() - x.timer < 3:  #if recepients still haven't received the previous message
                return

            x.timer = time.time()

            if x.retryCount > 4:
                print("*****************************")
                for recepient in x.intendedRecepients:
                    print("ERROR: Gave up sending to " + recepient)
                print("*****************************")
                
                if self.clientID == None:
                    print("Failed to register user.")
                    sys.exit()
                else:
                    self.ackMessage.remove(x)
                    continue

            self.networkOutput.append(x)
            x.retryCount += 1
            continue

    def RegisterClient(self):
        registrationValues = ["000", "999", str(Protocol.RegistrationRequest.value), "1", str(self.messageID), "", "register"]
        registrationMessage = Message(FormatMessage(registrationValues))
        registrationMessage.intendedRecepients = ['999']
        registrationMessage.timer = time.time()
        self.networkOutput.append(registrationMessage)
        self.ackMessage.append(registrationMessage)
        self.messageID += 1

    def Process(self):
        self.clientSocket.bind(("0.0.0.0", 63683))
        watch_for_write = []
        watch_for_read = [sys.stdin, self.clientSocket]
        self.RegisterClient()
        while True:
            try:
                if len(self.networkOutput) > 0:
                    watch_for_write = [self.clientSocket]
                else:
                    watch_for_write = []
                
                input_ready, output_ready, except_ready = select.select(watch_for_read, watch_for_write, watch_for_read, 3)    
                
                for item in input_ready:
                    if item == sys.stdin:
                        data = sys.stdin.readline().strip()
                        if len(data) > 0:
                            self.localInput.append(str(data))

                    if item == self.clientSocket:
                        data, addr = self.clientSocket.recvfrom(1024)
                        data = str(data)
                        
                        if len(data) < 0:
                            return

                        regEx = re.compile("^(SRC:[0-9]{3};DST:[0-9]{3};PNUM:[0-9]{1};HCT:[0-9]{1};MNUM:[0-9]{3};VL:(\d+(,\d+)*)?;MESG:(.*))$")
                        if regEx.match(data):    # drop data if it doesn't match the given format
                            nwData = Message(data)
                            src = nwData.parsedMessage[FieldName[Attribute.Source]]
                            if src not in self.peerList:
                                pnum = int(nwData.GetTagValue(Attribute.ProtocolNumber))
                                if addr in self.peerList.values() and pnum == Protocol.DataConfirmation:
                                    nwData.parsedMessage[FieldName[Attribute.ViewLog]] = self.peerList.keys()[self.peerList.values().index(addr)]
                                    nwData.messageType = MessageType.Forward
                                else:
                                    self.unknownPeers[(src, nwData.parsedMessage[FieldName[Attribute.MessageIDNumber]])] = addr
                            self.networkInput.append(nwData)
                        
                        else:
                            print("********************")
                            print("Dropped message - format mismatch")
                            print("MESG: " + data)
                            print("********************")
                
                for item in output_ready:
                    if item == self.clientSocket:
                        try:
                            msg_to_send = self.networkOutput.popleft()
                            msgID = msg_to_send.parsedMessage[FieldName[Attribute.MessageIDNumber]]
                            for recepient in msg_to_send.intendedRecepients:
                                if recepient in self.peerList: 
                                    self.clientSocket.sendto(msg_to_send.Format(recepient), self.peerList[recepient])
                                
                                elif (recepient, msgID) in self.unknownPeers:
                                    self.clientSocket.sendto(msg_to_send.Format(recepient), self.unknownPeers[(recepient, msgID)])
                                    del self.unknownPeers[(recepient, msgID)]
                                
                                #print(msg_to_send.Format(recepient))
                        except IndexError:
                            pass
                        except Exception as e:
                            print("Unhandled send exception: %s" % e)
                
                for item in except_ready:
                    if item == self.clientSocket:
                        return
            
            # Catch ^C to cancel and end program.
            except KeyboardInterrupt:
                return
            except Exception as e:
                print("Unhandled exception 0: %s" % e)
                return

            self.HandleIO()

def main():
    serverConnectionManager = ConnectionHandler(("steel.isi.edu", 63682))
    serverConnectionManager.Process()

if __name__ == "__main__":
    main()
