module Chord 

#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
open System
open Akka.FSharp
open Akka.Actor


let mutable flag = true

(* Create an instance of System.Random for generating random numbers. *)
let r = System.Random()

(* Create an Akka.NET actor system with the name "system" using the configuration loaded from Akka.NET. *)
let system = System.create "system" (Configuration.load())


(* Define a custom message type QueryMsg for Chord communication. *)
type QueryMsg =
    {
        
        Source: IActorRef
        k: int
        Hop: int
    }

(*
   Definition of the FixfngrInfo type, which represents information related to fixing fingers in a Chord node.
*)
type FixfngrInfo =
    {
        TargetIndex: int
        TargetNode: IActorRef
        FingerFrom: IActorRef
        FingerKey: int
        FingerIndex: int
        IsReturn: bool
    }

(*
   Definition of the InfoNd type, representing information about a node in the Chord network.
*)
type InfoNd = 
    {
        Ndkey: int
        NdRef: IActorRef
    }

(*
   Definition of the InfoToJoin type, which encapsulates information needed for a new node to join the Chord network.
*)
type InfoToJoin =
    {
        Before: InfoNd
        Next: InfoNd
    }

(*
   Definition of the UpdateBefore type, used to update information about the node before the current node.
*)
type UpdateBefore =
    {
        UBefore: InfoNd
    }

(*
   Definition of the SetupInfo type, which contains initial setup information for a Chord node.
*) 
type SetupInfo =
    {
        SelfKey: int
        BInfoNd: InfoNd
        NInfoNd: InfoNd
        TableInfoNd: List<InfoNd>
    }


(* Custom hash function that calculates an integer hash value for a given string. *)
let customHashFunction (s: string) =
    // Convert the input string to a byte array using ASCII encoding.
    let b = System.Text.Encoding.ASCII.GetBytes(s)
    // Initialize a mutable variable to store the hash result.
    let mutable res = 0
    for i in 0 .. (b.Length - 1) do
        res <- res + (int b.[i])
    if res < 0 then
        res <- 0 - res
    // Return the calculated hash value as an integer.
    res


let mutable numberofnodes = 200
let mutable numberofrequests = 3
let mutable m = 20

let mutable maxN = 1 <<< m

let chordActor (mailbox: Actor<_>) =
    let mutable fingerTable = []
    let mutable fingerArray = [||]
    let mutable k = 0
    let mutable next = {Ndkey = 0; NdRef = mailbox.Self}
    let mutable before = {Ndkey = 0; NdRef = mailbox.Self}
    (* Function to find the closest node preceding the given ID in the Chord network. *)
    let closestBeforeNode id =

        let mutable temp = -1
    // Iterate through the finger table entries from the most significant to least significant bits.
        for i = m - 1 downto 0 do
        // Calculate 'a' which is the key of the current finger entry.
            let a = if fingerTable.[i].Ndkey < k then fingerTable.[i].Ndkey + maxN else fingerTable.[i].Ndkey
        // Calculate 'id1' which is the adjusted target ID for comparison.
            let id1 = if id < k then id + maxN else id
        // Check if the current finger entry 'a' is between 'k' and 'id1'.
            if temp = -1 && a > k && a < id1 then
                temp <- i
    // If no suitable finger entry is found, return self; otherwise, return the reference of the closest node.
        if temp = -1 then
            mailbox.Self
        else 
            fingerTable.[temp].NdRef

       
    (* Function to handle an incoming integer message. *)
    let handleIntMessage id =
    // Find the closest node preceding the given key.
        let target = closestBeforeNode id
    // If the target is the current actor itself, do nothing (print an empty line).
        if target = mailbox.Self then
            printfn ""
        else
        // Send the integer message to the target node.
            target <! id

    (* Handle a QueryMsg message. *)
    let handleQueryMsg q =
    // Find the target node for the query key.
        let target = closestBeforeNode q.k
        if target = mailbox.Self then
        // If the target is the current node, send the query message back to the source.
            q.Source <! q
        else
        // If the target is not the current node, forward the query message to the target node with updated hop count.
            target <! { k = q.k; Hop = q.Hop + 1; Source = q.Source }


    let handleInfoToJoin itj =
        before <- itj.Before
        next <- itj.Next
        for i = 0 to m - 1 do
            next.NdRef <! {FingerKey = k + (1 <<< i); FingerIndex = i; FingerFrom = mailbox.Self; IsReturn = false; TargetIndex = -1; TargetNode = mailbox.Self}

    let handleSetupInfo info =
        k <- info.SelfKey
        next <- info.BInfoNd
        before <- info.NInfoNd
        fingerTable <- info.TableInfoNd

    (*
   Function to handle an incoming InfoNd message.
   InfoNd messages contain information about a new node that wants to join the Chord network.
*)
    let handleInfoNd info =
    // Find the closest node that precedes the InfoNd node's key.
        let target = closestBeforeNode info.Ndkey

    // Check if the closest node is the current actor (self).
        if target = mailbox.Self then
        // If the current actor is the closest, prepare an InfoToJoin message to send back to the new node.
            let itj = { Before = { Ndkey = k; NdRef = mailbox.Self }; Next = { Ndkey = next.Ndkey; NdRef = next.NdRef } }

        // Send the InfoToJoin message to the new node.
            info.NdRef <! itj

        // Update the 'next' node reference to the new node.
            next.NdRef <! { UBefore = info }
            next <- info

        // Populate the finger table of the new node with appropriate entries.
            for i = 0 to m - 1 do
                next.NdRef <! { FingerKey = k + (1 <<< i); FingerIndex = i; FingerFrom = mailbox.Self; IsReturn = false; TargetIndex = -1; TargetNode = mailbox.Self }
        else
        // If the closest node is not the current actor, forward the InfoNd message to the closest node.
            target <! info


    (* Function to handle FixfngrInfo messages. *)
    let handleFixFingerInfo (fi: FixfngrInfo) =
        if not fi.IsReturn then
        // Find the target node for the given FingerKey.
            let target = closestBeforeNode fi.FingerKey
            if target = mailbox.Self then
            // If the target is the current node, send a response to the FingerFrom node.
                fi.FingerFrom <! { FingerFrom = fi.FingerFrom; FingerKey = fi.FingerKey; FingerIndex = fi.FingerIndex; IsReturn = true; TargetIndex = next.Ndkey; TargetNode = next.NdRef }
            else
            // Otherwise, forward the FixfngrInfo message to the target node.
                target <! fi
        else
        // If the IsReturn flag is true, update the corresponding entry in the fingerArray.
            Array.set fingerArray fi.FingerIndex { Ndkey = fi.TargetIndex; NdRef = fi.TargetNode }


    let rec loop() = actor {
        let! message = mailbox.Receive()
        match box message with
            | :? int as id            -> handleIntMessage id
            | :? QueryMsg as q        -> handleQueryMsg q
            | :? InfoToJoin as itj    -> handleInfoToJoin itj
            | :? SetupInfo as info    -> handleSetupInfo info
            | :? InfoNd as info       -> handleInfoNd info
            | :? FixfngrInfo as fi    -> handleFixFingerInfo fi
            | :? string as x          -> printfn "%A" x
            | :? UpdateBefore as u    -> before <- u.UBefore
            | _                       -> ()
        return! loop()
    }
    loop()

let mutable numberList = []

let mutable chrdactrlist= []
let mutable inputStringList = [[]]

(* Define a boss actor responsible for managing the Chord simulation. *)
let createActor =
    spawn system "BossActor" 
        (fun mailbox ->
            let mutable totalnumofHops = 0
            let mutable messageCounter = 0
            
            (* Function to handle string messages (e.g., "begin"). *)
            let handleStringMessage s =
                if s = "begin" then
                    for i in 0 .. (numberofnodes - 1) do
                        for j in 0 .. (numberofrequests - 1) do
                            // Send a query message to each Chord actor.
                            chrdactrlist.[i] <! { k = (customHashFunction inputStringList.[i].[j]) % maxN; Hop = 0; Source = mailbox.Self }

            (* Function to handle query messages. *)
            let handleQueryMsg q =
                totalnumofHops <- totalnumofHops + q.Hop
                messageCounter <- messageCounter + 1
                if messageCounter = numberofnodes * numberofrequests then
                    // Calculate and print the average hops.
                    printfn "Average no.of Hops: %f" ((float totalnumofHops) / (float messageCounter))
                    System.Environment.Exit(0)

            (* Define the main loop of the boss actor. *)
            let rec loop() = actor {
                let! message = mailbox.Receive()
                match box message with
                    | :? string as s   -> handleStringMessage s
                    | :? QueryMsg as q -> handleQueryMsg q
                    | _                -> ()
                return! loop()
            }
            loop()
        )


(* Function to generate a list of FingerNodes for a Chord node. *)
let generateFingerList (i: int) (m: int) (numberofnodes: int) (maxN: int) (numberList: int list) (chrdactrlist: IActorRef list) =
    // Initialize an empty list to store the FingerNodes.
    [
        for j in 0 .. (m-1) do
            let mutable mrk = -1
            let temp = 1 <<< j
            for k in 1 .. numberofnodes do
                let a = if i + k >= numberofnodes then numberList.[(i+k) % numberofnodes] + maxN else numberList.[(i+k) % numberofnodes]
                if a >= temp + numberList.[i] && mrk = -1 then
                    mrk <- (i+k) % numberofnodes
            // Yield a FingerNode with the calculated key and reference to the corresponding actor.
            yield { Ndkey = numberList.[mrk]; NdRef = chrdactrlist.[mrk] }
    ]


(* Generates a list of random strings to simulate requests. *)
let generateRandomRequestList (numberofrequests: int) (r: System.Random) =
    [
        for j in 1 .. numberofrequests do
            let mutable str = "" // Initialize an empty string to build the request.
            let l = r.Next() % 100 + 1 // Generate a random length for the string (between 1 and 100 characters).
            for k in 0 .. l do
                str <- str + char(r.Next() % 95 + 32).ToString() // Append a random character to the string.
            yield str // Yield the generated random string.
    ]




/// Initializes the Chord Distributed Hash Table (DHT) system.
let initializeChordSystem peerNodes requests  =
    // Convert the peerNodes and reqN to integers and store them in numberofnodes and numberofrequests respectively.
    numberofnodes <- int peerNodes
    numberofrequests <- int requests

    // Compute the bit length (m) of the maximum number of nodes.
    m <- 0
    let mutable n = numberofnodes
    while n > 0 do
        m <- m + 1
        n <- (n >>> 1)
    // Calculate the maximum possible node ID.
    maxN <- 1 <<< m

    // Generate a list of random node IDs.
    let rndnumberList = [
       for i in 1 .. numberofnodes do
            yield (r.Next() % maxN)
    ]
    // Sort the node IDs.
    numberList <- List.sort rndnumberList

    // Spawn an actor for each node in the Chord system.
    let mutable i = 0
    while i < numberofnodes do
        let name = "Actor" + i.ToString()
        let temp = spawn system name chordActor
        chrdactrlist <- temp :: chrdactrlist
        i <- i + 1

    // Initialize each actor with its appropriate Chord parameters.
    for i in 0 .. (numberofnodes - 1) do
        let FList = generateFingerList i m numberofnodes maxN numberList chrdactrlist
        let myInfo = {SelfKey = numberList.[i]; BInfoNd = {Ndkey = numberList.[(i+1) % numberofnodes]; NdRef = chrdactrlist.[(i+1) % numberofnodes]}; NInfoNd = {Ndkey = numberList.[(i+numberofnodes-1) % numberofnodes]; NdRef = chrdactrlist.[(i+numberofnodes-1) % numberofnodes]}; TableInfoNd = FList}
        chrdactrlist.[i] <! myInfo

    // Generate a request list for each node.
    inputStringList <- [
        for i in 1 .. numberofnodes do
            let rqstList = generateRandomRequestList numberofrequests r
            yield rqstList
    ]

    // Start the BossActor which oversees the operations.
    createActor <! "begin"
    // Infinite loop to keep the system running (can be removed or replaced with more meaningful logic).
    let mutable x = 1 
    while true do
        x <- 1
