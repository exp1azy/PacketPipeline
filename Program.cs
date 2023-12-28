namespace PacketDataIndexer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);
            builder.Services.AddWindowsService(options =>
            {
                options.ServiceName = "PacketDataIndexer";
            });
            builder.Services.AddHostedService<PacketPipeline>();

            var host = builder.Build();
            host.Run();
        }
    }
}