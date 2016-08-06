Imports TPLDataflow.ProducerConsumerLayer

Module Module1

    Sub Main()
        Dim tasks As IEnumerable(Of Task) = New List(Of Task)

        Try
            dim producer = ProducerConsumerDataflow.StartProducerAsync()
            Dim consumer = ProducerConsumerDataflow.StartConsumerAsync()
            
            Task.WaitAll(tasks)
        Catch ex As Exception
            If (Typeof(ex) Is AggregateException) Then

                Dim aggregateException = DirectCast(ex, AggregateException)

                Dim flattenedException = aggregateException.Flatten()

                Console.ForegroundColor = ConsoleColor.Red
                Console.WriteLine(flattenedException.InnerException)
                Return
            End If

            Console.WriteLine(ex)
        End Try
  
        Console.ForegroundColor = ConsoleColor.White
        Console.WriteLine("Press any key to exit...")
        Console.ReadKey()        
    End Sub

End Module
