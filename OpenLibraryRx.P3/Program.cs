using System;

namespace OpenLibraryRx.P3
{
    internal static class Program
    {
        private static WebServer _server;

        private static void Main(string[] args)
        {
            const string prefix = "http://localhost:8080/";
            Console.Title = "Google Books Rx Server (P3)";
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Booting on {prefix}");

            _server = new WebServer(prefix);
            _server.Start(); // Accept na klasičnoj niti; obrada ide kroz Rx pipeline

            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Ready. Open your browser at {prefix}");
            Console.WriteLine("Press Enter to stop...");
            Console.ReadLine();

            _server.Dispose();
        }
    }
}
