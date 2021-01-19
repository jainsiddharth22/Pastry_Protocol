open Akka
open Akka.FSharp
open System
open System.Diagnostics
open Akka.Actor
open System


//Create System reference
let system = System.create "system" <| Configuration.defaultConfig()

let mutable numNodes = 0
let mutable numRequests = 0
let mutable arrayActor : IActorRef array = null
let mutable exitNodes = 0

let mutable messageGenerated = 0
let mutable messageDelivered = 0
let mutable totalHops = 0
let mutable flag = true


type Message() = 
    [<DefaultValue>] val mutable dst: int
    [<DefaultValue>] val mutable curr: int
    [<DefaultValue>] val mutable from: int
    [<DefaultValue>] val mutable hops: int

let killActor num = 
    arrayActor.[int num] <! PoisonPill.Instance

let killAll start = 
    for i = 0 to numNodes-1 do
        killActor i

let addZeros num zeroNum = 
    let mutable zeros = ""
    for i = 0 to zeroNum-1 do
        zeros <- zeros + "0"
    zeros + num

let rec int32ToBinary2 i =
    match i with
    | 0 | 1 -> string i
    | _ ->
        let bit = string (i % 2)
        (int32ToBinary2 (i / 2)) + bit

let int32ToBinary i = 
    let binNum = int32ToBinary2 i
    (addZeros binNum (32-binNum.Length))

let longestPrefixMatch num dest = 
    let num = (string) num
    let dest = (string) dest
    let mutable next = ""
    let mutable retVal = -1
    for i = 0 to 31 do
        if dest.[i] <> num.[i] then
            if retVal = -1 then
                retVal <- i
                next <- next + (string)dest.[i]
            else
                next <- next + "0"
        else
            next <- next + (string)num.[i]
    retVal, next

let binaryToInt32 binNum =
    let charToInt charNum = 
        if charNum = '1' then
            1
        else
            0
    let binNum = string binNum
    let mutable power = double 31
    let mutable total = double 0
    for i = 0 to 31 do
        total <- total + ((double) (charToInt binNum.[i]))*((double 2)**power)
        power <- power - double 1
    (int)total
    
let sendMessage dst curr from hops = 
    let sendMsg = new Message()
    sendMsg.dst <- dst
    sendMsg.curr <- curr
    sendMsg.from <- from
    sendMsg.hops <- hops
    arrayActor.[int curr] <! sendMsg
    
let route dst curr from =
    let binCurr = int32ToBinary curr
    let binDst = int32ToBinary dst

    let longestPrefixMatchNum, nextNode = (longestPrefixMatch binCurr binDst)
    sendMessage dst (binaryToInt32 nextNode) from


let getNeighbour currentNum = 
    let objrandom = new Random()
    let mutable ran = objrandom.Next(0,numNodes)
    while(ran = currentNum) do
        ran <- objrandom.Next(0,numNodes)
    ran


//Actor
let actor (actorMailbox:Actor<Message>) = 
    //Actor Loop that will process a message on each iteration
    let mutable count = 0
    let timer = new Stopwatch()
    let rec actorLoop() = actor {

        //Receive the message
        let! msg = actorMailbox.Receive()
        
        if count = 0 then
            timer.Start()
            count <- count + 1
        let currentTime = double (timer.ElapsedMilliseconds)
        
        if msg.curr <> msg.dst then
            route msg.dst msg.curr msg.from (msg.hops + 1)
        elif msg.curr = msg.dst && msg.from <> msg.curr then
            messageDelivered <- messageDelivered + 1
            totalHops <- totalHops + msg.hops
            printfn "Message Delivered %A" messageDelivered
            if messageDelivered = numNodes*numRequests && flag then
                timer.Stop()
                printfn "Done"
                printfn "Avg Hops %A" ((double)totalHops/(double)messageDelivered)
                killAll true

        elif currentTime > 1000.0 && count < numRequests + 1 then
            timer.Reset()
            timer.Start()
            count <- count + 1
            let toSend = getNeighbour msg.curr
            messageGenerated <- messageGenerated + 1
            printfn "Message Generated %A" messageGenerated
            route (toSend) (msg.curr) (msg.curr) 0
        
        if count < numRequests + 1 then
            sendMessage (msg.curr) (msg.curr) (msg.curr) 0
        
        return! actorLoop()
    }

    //Call to start the actor loop
    actorLoop()

let makeActors start =
    arrayActor <- Array.zeroCreate numNodes

    for i = 0 to numNodes-1 do
        let name:string = "actor" + i.ToString() 
        arrayActor.[i] <- spawn system name actor 



[<EntryPoint>]
let main (args) =
    numNodes <- args.[0] |> int
    numRequests <- args.[1] |> int
    
    makeActors true

    sendMessage 0 0 0 0

    System.Console.ReadKey() |> ignore

    0 // return an integer exit code
