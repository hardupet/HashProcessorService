using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Data.SqlClient;
using System.Text;

namespace HashProcessorService
{
    public class Worker : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration; 
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("The service has started............................................");
            return base.StartAsync(cancellationToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("The service has been stopped............................................");
            return base.StopAsync(cancellationToken);
        }
        
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var factory = new ConnectionFactory() { HostName = "localhost" };
                    using var connection = factory.CreateConnection();
                    using var channel = connection.CreateModel();
                    channel.QueueDeclare(queue: "hashes",
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

                    var consumer = new EventingBasicConsumer(channel);                   

                    Parallel.For(0, 4, i =>
                    {
                        while (true)
                        {
                            var ea = channel.BasicGet("hashes", autoAck: false);
                            if (ea == null)
                            {
                                Thread.Sleep(100);
                                continue;
                            }

                            var body = ea.Body.ToArray();
                            var message = Encoding.UTF8.GetString(body);
                            Console.WriteLine("Thread {0} received message: {1}", i, message);
                            _logger.LogInformation("Thread {0} received message: {1}", i, message);

                            // save message to database
                            var connectionString = _configuration.GetConnectionString("ConnectionString");
                            using var connection = new SqlConnection(connectionString);
                            connection.Open();

                            var insertQuery = "INSERT INTO hashes(date, sha1) VALUES(@date, @sha1)";
                            using var command = new SqlCommand(insertQuery, connection);
                            command.Parameters.AddWithValue("@date", DateTime.Now);
                            command.Parameters.AddWithValue("@sha1", message);
                            command.ExecuteNonQuery();


                            // process the message
                            channel.BasicAck(ea.DeliveryTag, multiple: false);
                        }
                    });

                    Console.WriteLine("Hash Processor started. Press [enter] to exit.");
                    Console.ReadLine();

                    _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                    await Task.Delay(1000, stoppingToken);
                }
            }
            catch (Exception ex)
            {

                _logger.LogError($"An error occured:=> {ex.Message}");
            }

        }
    }
}