module Program
open Chord
open System

[<EntryPoint>]
let main argv =
    // Extract the number of peers (nodes) and number of requests from command-line arguments.
    let peerNodes, requests =
        match argv with
        | [| peerNodes; requests |] -> int peerNodes, int requests
        | _ ->
            printfn "Usage: program.exe <number_of_peers> <number_of_requests>"
            0, 0 

    // Initialize the Chord Distributed Hash Table (DHT) system with the specified parameters.
    initializeChordSystem peerNodes requests

    // Return 0 to indicate a successful execution.
    0
