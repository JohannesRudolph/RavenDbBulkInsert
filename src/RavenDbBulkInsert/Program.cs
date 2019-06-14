using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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

      // Create a list of fake employees.
      var employee = Builder<Employee>.CreateListOfSize(1)
          .All()
          .With(x => x.FirstName, GetRandom.FirstName())
          .And(x => x.LastName, GetRandom.LastName())
          .Build()
          .Single();

      var employees = Enumerable.Repeat(employee, 10_000_000).ToList();

      Console.WriteLine("Created all fake employees.");

      var stopwatch = Stopwatch.StartNew();

      try
      {
        var opts = new ParallelOptions()
        {
          MaxDegreeOfParallelism = 512
        };

        Parallel.ForEach(employees, opts, e =>
        {
          BulkInsertEmployees(Enumerable.Repeat(e, 1), documentStore);
        });
      }
      catch (Exception exception)
      {
        Console.WriteLine(exception);
      }

      stopwatch.Stop();

      Console.WriteLine($"Inserted {employees.Count:N0} employee's in {stopwatch.Elapsed.TotalSeconds:N2} seconds.");

      Console.WriteLine("-- Press any key to quit.");
      Console.ReadKey();
    }

    private static void BulkInsertEmployees(IEnumerable<Employee> batch, IDocumentStore documentStore)
    {
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
