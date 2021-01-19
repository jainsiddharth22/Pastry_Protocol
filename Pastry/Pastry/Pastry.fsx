#I @"packages"
// #r "nuget: Akka.FSharp" 
// #r "nuget: Akka" 
#r "Akka.FSharp.dll"
#r "Akka.dll"
// #r "System.Configuration.ConfigurationManager.dll"
#r "Newtonsoft.Json.dll"
#r "FsPickler.dll"
#r "FSharp.Core.dll"

open Akka
open Akka.FSharp
open Akka.Actor
open System
open System.Diagnostics

let system = System.create "system" <| Configuration.load ()

type ProcessorMessage = 
    | IntializeParent of int*int*IActorRef
    | FirstJoin of int*string*int*int*int*IActorRef
    | Joined of int
    | Init of int
    | Join of int*string*int*int*int*IActorRef*string
    | AddMe of string*(string[,])*(int list)*(int list)*int
    | NextPeer of ActorSelection*(string[,])*(int list)*(int list)*int
    | Deliver of (string[,])*(int list)*(int list)
    | Print of bool
    | StartRouting of string*Set<int>
    | Forward of string*int*int
    | Finished of int

// let (s:int list) = [1..10]
// let (m:int list) = List.map (fun elem->elem+1) s
// printfn "%A" m

// let set1 = Set.empty.Add(3).Add(5).Add(7). Add(9)
// let mutable set2 = set1
// set2<-Set.remove 7 set2

// let ch = '7'
// let s= string ch
// //let x = Int32.Parse(s)
// printfn "%d" (int s)

// let ll = [1..10]
// printfn "%d" (List.last ll)
// printfn "%A" ll

// printfn "%A" set1

// let x = "0o71"
// printfn "x is %d" (int x)

let getAvailableActorsSet (numNodes:int) = 
    Set.ofSeq [ 0 .. numNodes-1 ]

let getRandArrElement =
  let rnd = Random()
  fun (arr : int list) -> arr.[rnd.Next(arr.Length)]

let maxBitsRequired (numNodes:int) (b:int) = int (Math.Ceiling(Math.Log(double numNodes)/Math.Log((Math.Pow(2.0,double b)))))

let toBaseString (num:int) (baseNo:int) (maxBits:int) = 
    let mutable str = Convert.ToString(num, baseNo);
    let diff = maxBits - str.Length
    if(diff>0) then
        let mutable j = 0
        while (j<diff) do
            str <- "0" + str
            j <- j+1
    str

let noOfRTColums b = 
    int (Math.Pow(2.0, double b))

let checkPrefix (str1:string) (str2:string) = 
    let mutable j = 0
    while(j<str1.Length && str1.[j]=str2.[j]) do
        j<-j+1
    j

// let x = checkPrefix "110023" "110223"
// printfn "%d" x
//let x = maxBitsRequired 10000 3.0
// let s = toBaseString 150 8 5
// printfn "%s" s

// let array2D = Array2D.init 3 3 (fun a b -> String.Empty) 
// array2D.[1,1] <- "boo"
// printfn "%A" array2D.[1,1]

let mutable nReq = 0

let child (mailbox: Actor<_>) = 
    let mutable b = 0
    let mutable l = 0
    let mutable childNodeNumber = 0
    let mutable childNodeBaseId = String.Empty
    let mutable parentRef = null
    let mutable maxBits = 0

    let mutable (smallLeafList: int list) = List.Empty
    let mutable (largeLeafList: int list) = List.Empty
    let mutable (routingTable:string[,]) = Array2D.init 1 1 (fun a b -> String.Empty)

    let listcontains intList elem = 
        List.exists (fun e -> e=elem) intList
    
    let removeLastElem (intList: int list) =
        let lastElem = List.last intList
        List.filter (fun elem -> elem<>lastElem) intList

    let updateNewNodeLarge (currIntID:int) (destID:string) (largeList: int list) =
        let destIntID = int destID
        let mutable largeListCopy = largeList
        let mutable isLargeFull = false

        if(largeListCopy.Length=l/2) then
            isLargeFull <- true

        if(currIntID>destIntID) then
            if(not (listcontains largeListCopy currIntID)) then
                if (isLargeFull) then
                    largeListCopy <- List.append largeListCopy [currIntID]
                    largeListCopy <- List.sort largeListCopy
                    largeListCopy <- removeLastElem largeListCopy
                else
                    largeListCopy <- List.append largeListCopy [currIntID]
                    largeListCopy <- List.sort largeListCopy

        largeListCopy

    let updateNewNodeSmall (currIntID:int) (destID:string) (smallList: int list) =
        let destIntID = int destID
        let mutable smallListCopy = smallList
        let mutable isSmallFull = false

        if(smallListCopy.Length=l/2) then
            isSmallFull <- true

        if(currIntID<destIntID) then
            if(not (listcontains smallListCopy currIntID)) then
                if (isSmallFull) then
                    smallListCopy <- List.append smallListCopy [currIntID]
                    smallListCopy <- List.sort smallListCopy
                    smallListCopy <- List.tail smallListCopy
                else
                    smallListCopy <- List.append smallListCopy [currIntID]
                    smallListCopy <- List.sort smallListCopy

        smallListCopy

    let updateSelfRT (level:int) (destID:string) = 
        //printfn "Inside updateSelfRT dlevel is %d & string is %s" level destID

        let dLevel = int (string (destID.[level]))
        if(String.IsNullOrEmpty routingTable.[level,dLevel]) then
            routingTable.[level,dLevel] <- destID
        else
            let originalDiff = Math.Abs ((int ("0o"+routingTable.[level,dLevel]))-(int ("0o"+childNodeBaseId)))
            let newDiff = Math.Abs (int ("0o"+destID) - int ("0o"+childNodeBaseId))
            if(newDiff<originalDiff) then
                routingTable.[level,dLevel] <- destID

    let updateNewNodeRT (destID:string) (level:int) (rT: string[,]) (lastLevel:int) = 
        let mutable (newRT: string[,]) = rT
        // printfn "xxxxxxxxx"
        // printfn "last level:%d" (lastLevel)
        // printfn "level:%d" (level)
        for i=lastLevel to level do
            for j = 0 to (Array2D.length2 routingTable - 1) do
                //if(not (String.IsNullOrEmpty routingTable.[i,j])) then
                    newRT.[i,j] <- routingTable.[i,j]
        
        let mutable dLevel = int (string childNodeBaseId.[level])
        // printfn "dLevel:%d" (dLevel)
        // printfn "elem:%b" (String.IsNullOrEmpty (newRT.[level,dLevel]))
        // printfn "yyyyyyyyy"
        if(not (String.IsNullOrEmpty newRT.[level,dLevel])) then
            let originalDiff = Math.Abs ((int ("0o"+newRT.[level,dLevel]))-(int ("0o"+destID)))
            let newDiff = Math.Abs (int ("0o"+destID) - int ("0o"+childNodeBaseId))
            if(newDiff<originalDiff) then
                newRT.[level,dLevel] <- childNodeBaseId
            // let maxID = int (Math.Max (double newRT.[level,dLevel],double childNodeBaseId))
            // newRT.[level,dLevel] <- toBaseString (int("0o"+(string maxID))) (int (Math.Pow(2.0,double b))) maxBits
        else
            newRT.[level,dLevel] <- childNodeBaseId
        
        dLevel <- int (string destID.[level])
        newRT.[level,dLevel] <- String.Empty
        newRT

    let updateLeafLists (level:int) (destID:string) = 
        //printfn "inside update leaflists"
        let destIntID = int destID
        let currIntID = int childNodeBaseId

        let mutable isLargeFull = false
        let mutable isSmallFull = false

        if (largeLeafList.Length = l/2) then
            isLargeFull <- true

        if (smallLeafList.Length = l/2) then
            isSmallFull <- true

        if (destIntID>currIntID) then
            if (not (listcontains largeLeafList destIntID)) then
                if (isLargeFull) then
                    largeLeafList <- List.append largeLeafList [destIntID]
                    largeLeafList <- List.sort largeLeafList
                    largeLeafList <- removeLastElem largeLeafList
                else
                    largeLeafList <- List.append largeLeafList [destIntID]
                    largeLeafList <- List.sort largeLeafList
        else
            if (not (listcontains smallLeafList destIntID)) then
                if (isSmallFull) then
                    smallLeafList <- List.append smallLeafList [destIntID]
                    smallLeafList <- List.sort smallLeafList
                    smallLeafList <- List.tail smallLeafList
                else
                    smallLeafList <- List.append smallLeafList [destIntID]
                    smallLeafList <- List.sort smallLeafList               
        //printfn "exit leaflists"


    let route (destID:string) (level:int) (func:string) = 
        let mutable found = false
        let mutable nextID = String.Empty
        let mutable nextDestIntID = -1
        let mutable destIntID = int ("0o"+destID)
        let mutable currIntID = int ("0o"+childNodeBaseId)
        let mutable minDiff = Math.Abs (destIntID-currIntID)
        //let mutable isNextAlive = false

        if (level=maxBits) then
            nextID <- null
            found <- true

        if(not found) then
            let mutable leaves = List.empty
            let mutable sl = smallLeafList
            sl<- List.map (fun elem->(int ("0o"+(string elem)))) sl
            leaves<- List.append leaves sl
            
            leaves<-List.append leaves [currIntID]

            let mutable ll = largeLeafList
            ll<- List.map (fun elem->(int ("0o"+(string elem)))) ll
            leaves<- List.append leaves ll

            if(destIntID>=List.head leaves && destIntID<=List.last leaves) then
                let mutable nextDestIntID = List.head leaves
                let mutable d = Math.Abs (nextDestIntID-destIntID)
                for i in leaves do
                    if(Math.Abs (i-destIntID)<d) then
                        nextDestIntID<-i
                        d<-Math.Abs (i-destIntID)

        if(nextDestIntID=currIntID) then
            nextID<-null
            found<-true
        else if(nextDestIntID<>(-1)) then
            nextID<-toBaseString nextDestIntID (int (Math.Pow(2.0,double b))) maxBits
            found<-true

        //check to see if any node in the leaf sets is closer to the new node than the current node
        // if (not found) then
        //     //search in leaf tables first
        //     //nextDestIntID<-currIntID
        //     if(destIntID>currIntID) then
        //         if(not (List.isEmpty largeLeafList)) then
        //             if(func = "join") then
        //                 if(destIntID<int("0o" + string (List.last largeLeafList))) then
        //                     nextDestIntID<-currIntID
        //                     let mutable d = destIntID-currIntID
        //                     for i in largeLeafList do
        //                         if(Math.Abs (int ("0o"+(string i))-destIntID)<d) then
        //                             d<-Math.Abs (int ("0o"+(string i))-destIntID)
        //                             nextDestIntID<-int ("0o" + string i)
        //             else
        //                 if(destIntID<=int("0o" + string (List.last largeLeafList))) then
        //                     nextDestIntID<-currIntID
        //                     let mutable d = destIntID-currIntID
        //                     for i in largeLeafList do
        //                         if(Math.Abs (int ("0o"+(string i))-destIntID)<d) then
        //                             d<-Math.Abs (int ("0o"+(string i))-destIntID)
        //                             nextDestIntID<-int ("0o" + string i)
        //     else
        //         if(not (List.isEmpty smallLeafList)) then
        //             if(func = "join") then
        //                 if(destIntID>int("0o" + string (List.head smallLeafList))) then
        //                     nextDestIntID<-currIntID
        //                     let mutable d = currIntID-destIntID
        //                     for i in smallLeafList do
        //                         if(Math.Abs (int ("0o"+(string i))-destIntID)<d) then
        //                             d<-Math.Abs (int ("0o"+(string i))-destIntID)
        //                             nextDestIntID<-int ("0o" + string i)
        //             else
        //                 if(destIntID>=int("0o" + string (List.head smallLeafList))) then
        //                     nextDestIntID<-currIntID
        //                     let mutable d = currIntID-destIntID
        //                     for i in smallLeafList do
        //                         if(Math.Abs (int ("0o"+(string i))-destIntID)<d) then
        //                             d<-Math.Abs (int ("0o"+(string i))-destIntID)
        //                             nextDestIntID<-int ("0o" + string i)
        
        // if(nextDestIntID<>(-1) && nextDestIntID<>currIntID) then  
        //     nextID <- toBaseString nextDestIntID (int (Math.Pow(2.0,double b))) maxBits
        //     found <- true
        // // if(nextDestIntID<>currIntID) then
        // //     nextID <- toBaseString nextDestIntID (int (Math.Pow(2.0,double b))) maxBits
        // //     found <- true
        

        // if(nextDestIntID=currIntID) then
        //     nextID <- null
        //     found <- true

        //printfn "aaaaaaaaa"
        if(not found) then
            let colNo = int (string destID.[level])
            // printfn "level %d" level
            // printfn "routing table x:%d" (Array2D.length1 routingTable)
            // printfn "routing table y:%d" (Array2D.length2 routingTable)
            // printfn "colno: %d" colNo 

            if(not (String.IsNullOrEmpty (routingTable.[level,colNo]))) then
                nextID <- routingTable.[level,colNo]
                found<-true

        //printfn "bbbbbbbbb"
            // if(not (isNull (routingTable.[level-1,colNo]))) then
            //     if(func = "join") then
            //         if(int (routingTable.[level-1,colNo])<destIntID) then   
            //             nextID <- routingTable.[level-1,colNo]
            //         else
            //             nextID <- null
            //         found<-true
            //     else
            //         if(int (routingTable.[level-1,colNo])<destIntID) then   
            //             nextID <- routingTable.[level-1,colNo]
            //             found <- true

        if(not found) then
            let mutable eligibleNodes = Set.empty

            for i in largeLeafList do
                let l = toBaseString (int("0o"+(string i))) (int (Math.Pow(2.0,double b))) maxBits
                if((checkPrefix l destID)>=level) then
                    eligibleNodes <- Set.add (int("0o"+(string i))) eligibleNodes

            for i in smallLeafList do
                let l = toBaseString (int("0o"+(string i))) (int (Math.Pow(2.0,double b))) maxBits
                if((checkPrefix l destID)>=level) then
                    eligibleNodes <- Set.add (int("0o"+(string i))) eligibleNodes

            for i=level to (Array2D.length1 routingTable - 1) do
                for j=0 to (Array2D.length2 routingTable - 1) do
                    if(not (String.IsNullOrEmpty routingTable.[i,j])) then
                        eligibleNodes <- Set.add (int ("0o"+routingTable.[i,j])) eligibleNodes
            

            for i in eligibleNodes do
                if (i<>destIntID) then
                    let mutable diff = Math.Abs (destIntID-i)
                    if(diff<minDiff) then
                        minDiff <- diff
                        nextDestIntID <- i
                        found <- true

            if found then
                nextID <- toBaseString nextDestIntID (int (Math.Pow(2.0,double b))) maxBits
            else
                nextID <- null
                found <- true
        //printfn "ccccccccc"
        nextID            

    let rec loop () = actor {
        let! message = mailbox.Receive ()
        match message with
        | FirstJoin(childNo,childBaseId,bValue,lValue,maxB,pRef) -> 
            b<-bValue
            l<-lValue
            parentRef<-pRef
            childNodeNumber<-childNo
            childNodeBaseId<-childBaseId
            maxBits<-maxB

            routingTable <- Array2D.init maxBits (noOfRTColums b) (fun a b -> String.Empty)
            
            parentRef<!Joined(childNodeNumber)

        | Join(childNo,childBaseId,bValue,lValue,maxB,pRef,peerBaseId) ->
            b<-bValue
            l<-lValue
            parentRef<-pRef
            childNodeNumber<-childNo
            childNodeBaseId<-childBaseId
            maxBits<-maxB
            routingTable <- Array2D.init maxBits (noOfRTColums b) (fun a b -> String.Empty)
            
            let peerRef = system.ActorSelection("akka://system/user/"+peerBaseId)
            //printfn "%A" peerRef
            peerRef<!AddMe(childNodeBaseId,routingTable,smallLeafList,largeLeafList,0)

        | AddMe(newNodeBaseId,newNodeRT,newNodeSLS,newNodeLLS,lev) ->
            let mutable isLastHop = false
            let mutable dRT = newNodeRT
            let mutable dSLS = newNodeSLS
            let mutable dLLS = newNodeLLS

            let level = checkPrefix newNodeBaseId childNodeBaseId
            //printfn "newNode:%s & childNode:%s" newNodeBaseId childNodeBaseId
            //If only one node in the network
            if (List.isEmpty smallLeafList && List.isEmpty largeLeafList) then
                isLastHop <- true
            
            let next = route newNodeBaseId level "join"
            if(isNull next) then isLastHop<-true

            updateLeafLists level newNodeBaseId
            updateSelfRT level newNodeBaseId

            dRT <- updateNewNodeRT newNodeBaseId level newNodeRT lev

            if isLastHop then
                dSLS <- updateNewNodeSmall (int childNodeBaseId) newNodeBaseId dSLS
                dLLS <- updateNewNodeLarge (int childNodeBaseId) newNodeBaseId dLLS

                for leaf in smallLeafList do
                    dSLS <- updateNewNodeSmall leaf newNodeBaseId dSLS
                    dLLS <- updateNewNodeLarge leaf newNodeBaseId dLLS
        
                for leaf in largeLeafList do
                    dSLS <- updateNewNodeSmall leaf newNodeBaseId dSLS
                    dLLS <- updateNewNodeLarge leaf newNodeBaseId dLLS
                mailbox.Sender () <! Deliver(dRT,dSLS,dLLS)
            else
                let nextPeerRef = system.ActorSelection("akka://system/user/"+next) 
                mailbox.Sender () <! NextPeer(nextPeerRef,dRT,dSLS,dLLS,level)

        | NextPeer(nextPeerRef,dRT,dSLS,dLLS,level) ->
            routingTable <- dRT
            smallLeafList <- dSLS
            largeLeafList <- dLLS
            nextPeerRef <! AddMe(childNodeBaseId,routingTable,smallLeafList,largeLeafList,level)
            
        | Deliver(dRT,dSLS,dLLS) ->
            routingTable <- dRT
            smallLeafList <- dSLS
            largeLeafList <- dLLS
            // printfn "Child node no:%s" childNodeBaseId
            // printfn "LLS:%A" largeLeafList
            // printfn "SLS:%A" smallLeafList
            // printfn "RT:%A" routingTable 
            parentRef <! Joined(childNodeNumber)

        | Print(t) ->
            printfn "Child Node No:%s" childNodeBaseId
            printfn "LLS %A" largeLeafList
            printfn "SLS %A" smallLeafList
            printfn "RT %A" routingTable

        | StartRouting(msg,joinedActors) ->
            for i=1 to nReq do
                let key = getRandArrElement (Set.toList joinedActors)
                let keyBaseID = toBaseString key (int (Math.Pow(2.0,double b))) maxBits
                let level = checkPrefix keyBaseID childNodeBaseId
                mailbox.Self <! Forward(keyBaseID,level,0)

        | Forward(destID,level,noOfHops) ->
            let mutable nHops = noOfHops
            let nextID = route destID level "route"

            

            if(isNull nextID) then
                parentRef<! Finished (nHops)
            else
                //Threading.Thread.Sleep(1000)
                nHops<-nHops+1
                let newLevel = checkPrefix destID nextID
                let nextHop = system.ActorSelection("akka://system/user/"+nextID) 
                nextHop<!Forward(destID,newLevel,nHops)

        return! loop()
    }
    loop()

let parent (mailbox: Actor<_>) =
    let b = 3
    let l = 8
    let mutable maxBits = 0
    let mutable baseNo = int (Math.Pow(2.0,double b))
    let mutable availableNodes = Set.empty
    let mutable joinedNodes = Set.empty
    let mutable numActorsJoined = 0
    let mutable totalNodes = 0
    let mutable totalRequests = 0
    let mutable parentRef = null
    let mutable terminateCount = 0
    let mutable (totalHops:double) = 0.0

    let rec loop () = actor {

        let! message = mailbox.Receive ()
        match message with
        | IntializeParent(numNodes,numRequests,selfRef) -> 
            
            availableNodes <- getAvailableActorsSet numNodes
            totalNodes <- numNodes
            parentRef <- selfRef
            maxBits <- maxBitsRequired numNodes b
            totalRequests <- numRequests
            nReq <- totalRequests

            //printfn "maxbits: __________________________________ %d" maxBits
            
            let firstNodeNumber = getRandArrElement (Set.toList availableNodes)
            let firstNodeBaseId = toBaseString firstNodeNumber baseNo maxBits
            let firstNodeRef = spawn system firstNodeBaseId child
            //printfn "%s" (string firstNodeRef.Path)
            firstNodeRef<!FirstJoin(firstNodeNumber,firstNodeBaseId,b,l,maxBits,parentRef)

        | Joined(childNodeNo) ->
            numActorsJoined<-numActorsJoined+1
            availableNodes <- Set.remove childNodeNo availableNodes
            joinedNodes <- Set.add childNodeNo joinedNodes

            if(numActorsJoined<>totalNodes) then
                //printfn "Child Joined:%d" childNodeNo
                parentRef<!Init(childNodeNo)
            else
                //printfn "Done"
                Threading.Thread.Sleep(1000)
                //println(numNodes + " nodes have joined ...\n")
                //println("Initializing Routing ...\n")

                for i in joinedNodes do
                    let msg = "Hello"
                    let id = toBaseString i (int (Math.Pow(2.0,double b))) maxBits
                    let peerReference = system.ActorSelection("akka://system/user/"+id) 
                    peerReference<!StartRouting(msg,joinedNodes)
                   
                // for i=0 to totalNodes-1 do
                //     let id = toBaseString i (int (Math.Pow(2.0,double b))) maxBits
                //     let nextPeerRef = system.ActorSelection("akka://system/user/"+id) 
                //     nextPeerRef<!Print(true)
                //     Threading.Thread.Sleep(100) 

                    

        | Init(childNodeNo) ->
            let peerId = getRandArrElement (Set.toList joinedNodes)
            let peerBaseId = toBaseString peerId baseNo maxBits
            
            let newPeerId = getRandArrElement (Set.toList availableNodes)
            let newPeerBaseId = toBaseString newPeerId baseNo maxBits
            let newNodeRef = spawn system newPeerBaseId child
            //printfn "%s" newPeerBaseId
            newNodeRef<!Join(newPeerId,newPeerBaseId,b,l,maxBits,parentRef,peerBaseId)

        | Finished(nHops) ->
            terminateCount<-terminateCount+1
            totalHops<-totalHops+(double nHops)
            //printfn "%d" terminateCount
            if(terminateCount>=(totalNodes*totalRequests)) then
                Threading.Thread.Sleep(1000)
                printfn "All nodes have finished routing"
                printfn "Total routes = %d" (totalNodes * totalRequests)
                printfn "Total hops = %f" totalHops
                printfn "Average hops per route = %f" (totalHops / (double (totalNodes * totalRequests)))

                printfn ""
                printfn "Press Any Key To Close"

                //Close all actors
                system.Terminate() |> ignore

        return! loop()
    }
    loop()

//Get the arguments
let args : string array = fsi.CommandLineArgs |> Array.tail

//Extract and convert to Int
let numNodes = args.[0]|> int
let numReq = args.[1]|> int


let parentActor = spawn system "parent" parent
parentActor <! IntializeParent(numNodes,numReq,parentActor)

System.Console.ReadKey() |> ignore

// let parentActor = spawn system "parent" parent
// parentActor <! IntializeParent(10000,1,parentActor)