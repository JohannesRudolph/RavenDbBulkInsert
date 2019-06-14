using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FizzWare.NBuilder.Generators;
using MoreLinq;
using Raven.Client.Documents;

namespace RavenDbBulkInsert
{
  public class Program
  {
    public static async Task Main(string[] args)
    {
      Console.WriteLine("Starting bulk insert testing app.");

      var documentStore = new DocumentStore()
      {
        Urls = new[] { "http://localhost:4040" },
        Database = "BulkInsertTest"
      }.Initialize();


      var iterations = Enumerable.Range(1, 1_000_000).ToList();

      Console.WriteLine("Created all fake employees.");

      var stopwatch = Stopwatch.StartNew();

      try
      {
        var opts = new ParallelOptions()
        {
          MaxDegreeOfParallelism = 32
        };

        var i = 0;
        Parallel.ForEach(iterations, opts, e =>
        {
          var x = Interlocked.Increment(ref i);
          if (x % 2 == 0)
          {
            // BulkInsertEmployees(8192, documentStore);
            StoreEmployees(8192, documentStore);
          }
          else
          {
            StoreEmployees(8192, documentStore);
          }
        });
      }
      catch (Exception exception)
      {
        Console.WriteLine(exception);
      }

      stopwatch.Stop();

      Console.WriteLine($"Inserted {iterations.Count:N0} employee's in {stopwatch.Elapsed.TotalSeconds:N2} seconds.");

      Console.WriteLine("-- Press any key to quit.");
      Console.ReadKey();
    }

    private static void StoreEmployees(int count, IDocumentStore documentStore)
    {

      Console.WriteLine("Store");
      // Create a list of fake employees.
      var batch = Builder<Employee>.CreateListOfSize(count)
          .All()
          .With(x => x.FirstName, GetRandom.FirstName())
          .And(x => x.LastName, GetRandom.LastName())
          .Build();

      using (var operation = documentStore.OpenSession())
      {
        foreach (var employee in batch)
        {
          operation.Store(employee);
        }

        operation.SaveChanges();
      }
    }

    private static void BulkInsertEmployees(int count, IDocumentStore documentStore)
    {

      Console.WriteLine("Bulk");
      // Create a list of fake employees.
      var batch = Builder<Employee>.CreateListOfSize(count)
          .All()
          .With(x => x.FirstName, GetRandom.FirstName())
          .And(x => x.LastName, GetRandom.LastName())
          .Build();

      using (var operation = documentStore.BulkInsert())
      {
        foreach (var employee in batch)
        {
          operation.Store(employee);
        }
      }
    }
  }
}
