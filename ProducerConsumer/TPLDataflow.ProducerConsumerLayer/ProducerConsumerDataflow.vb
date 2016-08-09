Imports System.Threading
Imports System.Threading.Tasks.Dataflow

Public NotInheritable Class ProducerConsumerDataflow
    Private ReadOnly Shared messageBlock As New BufferBlock(Of Message)
    Private ReadOnly Shared producerTaskComplitionSource As New TaskCompletionSource(Of Object)
    Private Shared ReadOnly producerTaskCancellationTokenSource As New CancellationTokenSource()
    'Private Shared ReadOnly consumerTaskCancellationTokenSource As New CancellationTokenSource()
    Private Shared finalActionBlock As ActionBlock(Of FinalMessage)

    Public Shared Async Function StartProducerAsync() As Task

        Try
            Console.ForegroundColor = ConsoleColor.Green
            Console.WriteLine("Starting BufferBlock Producer...")
            Console.WriteLine()

            Dim index = 1

            ' Message Producer
            While True
                Dim message As Message = New Message($"Message {index}")

                Await messageBlock.SendAsync(message).ConfigureAwait(False)

                ' simulate a task processing time
                Await Task.Delay(100).ConfigureAwait(False)
                index = index + 1

                If (producerTaskCancellationTokenSource.IsCancellationRequested) Then

                    EndMessageBufferBlock()

                    producerTaskComplitionSource.SetResult(Nothing)
                    Exit While
                End If
            End While

        Catch ex As Exception
            ' Set the exception on the TaskCompletionSource
            producerTaskComplitionSource.SetException(ex)

            ' end the messageBufferBlock and give it enough time ro process any remaining items
            EndMessageBufferBlock()

            ' end the consumer and give it enough time to process remaining items
            finalActionBlock.Complete()
        End Try

        ' await the producer's tcs to get the set exception re-thrown
        Await producerTaskComplitionSource.Task.ConfigureAwait(False)
    End Function

    Private Shared Sub EndMessageBufferBlock()
        ' end the messageBufferBlock and give it enough time ro process any remaining items
        messageBlock.Complete()
        messageBlock.Completion.Wait()
    End Sub

    Public Shared Async Function StartConsumerAsync() As Task
        Dim tcs As TaskCompletionSource(Of Object) = New TaskCompletionSource(Of Object)

        Try
            Dim transformBlock As TransformBlock(Of Message, FinalMessage) =
                New TransformBlock(Of Message, FinalMessage)(Function(message)
                                                                 Console.ForegroundColor = ConsoleColor.DarkYellow
                                                                 Console.WriteLine($"TransformBlock Task:{Task.CurrentId} running on Thread:{Thread.CurrentThread.ManagedThreadId} got: {NameOf(message)} with {message.Data}")


                                                                 ' UNCOMMENT to test exception handling
                                                                 If (message.Data.Equals("Message 100")) Then
                                                                     Throw New InvalidOperationException($"Invalid Message: {message.Data}")
                                                                 End If

                                                                 Return New FinalMessage($"{message.Data} [Postprocessed]")
                                                             End Function)


            finalActionBlock =
                New ActionBlock(Of FinalMessage)(Sub(finalmessage)
                                                     Console.ForegroundColor = ConsoleColor.Green
                                                     Console.WriteLine($"FinalBlock Task:{Task.CurrentId} running on Thread:{Thread.CurrentThread.ManagedThreadId} got: {NameOf(finalmessage)} with {finalmessage.Data}")
                                                 End Sub)


            messageBlock.LinkTo(transformBlock, New DataflowLinkOptions With {.PropagateCompletion = True})
            transformBlock.LinkTo(finalActionBlock, New DataflowLinkOptions With {.PropagateCompletion = True})

            ' await the final block's task to get any exception re-thrown
            Await finalActionBlock.Completion.ConfigureAwait(False)

            ' when finalActionBlock task completes (issued in producer task on failure)
            tcs.SetResult(Nothing)
        Catch ex As Exception
            ' Set the exception on the TaskCompletionSource
            tcs.SetException(ex)

            ' end the producer Task
            producerTaskCancellationTokenSource.Cancel()
        End Try

        ' await the tcs to get the set exception re-thrown
        Await tcs.Task.ConfigureAwait(False)
    End Function
End Class
