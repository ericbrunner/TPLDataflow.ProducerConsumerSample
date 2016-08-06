Imports System.Threading
Imports System.Threading.Tasks.Dataflow

Public NotInheritable Class ProducerConsumerDataflow
    Private ReadOnly Shared messageBlock As New BufferBlock(Of Message)
    Private ReadOnly Shared producerTaskComplitionSource As New TaskCompletionSource(Of Object)
    

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

                If (producerTaskComplitionSource.Task.Status =TaskStatus.RanToCompletion) then
                    'Debugger.Break()
                    Exit While
                End If
            End While

        Catch ex As Exception
            producerTaskComplitionSource.SetException(ex)
        End Try

        Await producerTaskComplitionSource.Task.ConfigureAwait(False)
    End Function

    Public Shared Async Function StartConsumerAsync() As Task
        Dim tcs As TaskCompletionSource(Of Object) = New TaskCompletionSource(Of Object)

        Try
            Dim transformBlock As TransformBlock(Of Message, FinalMessage) =
                New TransformBlock(Of Message, FinalMessage)(Function(message)
                                                                 Console.ForegroundColor = ConsoleColor.DarkYellow
                                                                 Console.WriteLine($"TransformBlock Task:{Task.CurrentId} running on Thread:{Thread.CurrentThread.ManagedThreadId} got: {NameOf(Message)} with {message.Data}")


                                                                 ' UNCOMMENT to test exception handling
                                                                 If (message.Data.Equals("Message 100")) Then
                                                                     Throw New InvalidOperationException($"Invalid Message: {message.Data}")
                                                                 End If

                                                                 Return New FinalMessage($"{message.Data} [Postprocessed]")
                                                             End Function)


            Dim finalActionBlock As ActionBlock(Of FinalMessage) =
                New ActionBlock(Of FinalMessage)(Sub(finalmessage)
                                                     Console.ForegroundColor = ConsoleColor.Green
                                                     Console.WriteLine($"FinalBlock Task:{Task.CurrentId} running on Thread:{Thread.CurrentThread.ManagedThreadId} got: {NameOf(FinalMessage)} with {finalmessage.Data}")
                                                 End Sub)


            messageBlock.LinkTo(transformBlock, New DataflowLinkOptions With {.PropagateCompletion = True})
            transformBlock.LinkTo(finalActionBlock, New DataflowLinkOptions With {.PropagateCompletion = True})

            ' Any Exception thrown in the blocks results in a completed task in a faulted state
            Await finalActionBlock.Completion.ContinueWith(Sub(faultedTask)
                                                               
                                                               Console.WriteLine($"Boom! Task:{faultedTask.Id} exited in State:{faultedTask.Status}")
                                                               Throw faultedTask.Exception
                                                           End Sub).ConfigureAwait(False)
        Catch ex As Exception
            tcs.SetException(ex)

            ' end the producer task
            messageBlock.Complete()
            messageBlock.Completion.Wait()

            producerTaskComplitionSource.SetResult(Nothing)
            Console.WriteLine()
            Console.WriteLine($"producerTaskComplitionSource Task Status: {producerTaskComplitionSource.Task.Status}")
        End Try

        Await tcs.Task.ConfigureAwait(False)
    End Function
End Class
