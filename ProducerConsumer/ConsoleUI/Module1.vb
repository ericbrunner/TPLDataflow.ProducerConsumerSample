Imports TPLDataflow.ProducerConsumerLayer

Module Module1

    Sub Main()
        Dim tasks = New List(Of Task)

        Try
            dim producer = ProducerConsumerDataflow.StartProducerAsync()
            tasks.Add(producer)

            Dim consumer = ProducerConsumerDataflow.StartConsumerAsync()
            tasks.Add(consumer)

            Task.WaitAll(tasks.ToArray())

        Catch ex As Exception
            Console.ForegroundColor = ConsoleColor.Red

            If (Typeof(ex) Is AggregateException) Then

                Dim aggregateException = DirectCast(ex, AggregateException)
                Dim flattenedException = aggregateException.Flatten()

                Console.WriteLine(flattenedException.InnerException)
                Exit Try
            End If

            Console.WriteLine(ex)
        End Try
  
        Console.ForegroundColor = ConsoleColor.White
        Console.WriteLine()
        Console.WriteLine("Press any key to exit...")
        Console.ReadKey()        
    End Sub

End Module
