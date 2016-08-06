Imports System.Threading
Imports System.Threading.Tasks.Dataflow

Public NotInheritable Class ProducerConsumerDataflow
    Private Shared messageBlock As New BufferBlock(Of Message)

    Public Shared Async Function StartProducerAsync() As Task
        Dim tcs As TaskCompletionSource(Of Object) = New TaskCompletionSource(Of Object)

        Try
            Dim index = 1
            ' Message Producer
            While True
                Dim message As New Message($"hello world I am Message {index}")

                Await messageBlock.SendAsync(message)

                ' simulate a task processing latency time
                Await Task.Delay(100)
                index = index + 1
            End While

        Catch ex As Exception
            tcs.SetException(ex)
        End Try

        Await tcs.Task
    End Function

    Public Shared Async Function StartConsumerAsync() As Task
        Dim tcs As TaskCompletionSource(Of Object) = New TaskCompletionSource(Of Object)

        Try
            Dim transformBlock As TransformBlock(Of Message, FinalMessage) =
                New TransformBlock(Of Message, FinalMessage)(Function(message)
                                                                 Console.ForegroundColor = ConsoleColor.DarkYellow
                                                                 Console.WriteLine($"TransformBlock Task:{Task.CurrentId} running on Thread:{Thread.CurrentThread.ManagedThreadId} got a message: {message.Data}")
                                                                 Return New FinalMessage($"Postprocessed: {message.Data}")
                                                             End Function)


            Dim finalActionBlock As ActionBlock(Of FinalMessage) =
                New ActionBlock(Of FinalMessage)(Sub(finalmessage)
                                                     Console.ForegroundColor = ConsoleColor.Green
                                                     Console.WriteLine($"FinalBlock Task:{Task.CurrentId} running on Thread:{Thread.CurrentThread.ManagedThreadId} got a message: {finalmessage.Data}")
                                                 End Sub)


            messageBlock.LinkTo(transformBlock, New DataflowLinkOptions With {.PropagateCompletion = True})
            transformBlock.LinkTo(finalActionBlock, New DataflowLinkOptions With {.PropagateCompletion = True})

            ' Any Exception thrown in the blocks results in a completed task in a faulted state
            Await finalActionBlock.Completion

        Catch ex As Exception
            tcs.SetException(ex)
        End Try

        Await tcs.Task
    End Function
End Class
