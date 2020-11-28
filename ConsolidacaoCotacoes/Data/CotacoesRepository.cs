using System;
using System.Linq;
using System.Text.Json;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;
using StackExchange.Redis;
using ConsolidacaoCotacoes.Documents;

namespace ConsolidacaoCotacoes.Data
{
    public class CotacoesRepository
    {
        private readonly ConnectionMultiplexer _conexaoRedis;
        private readonly string _prefixoCotacaoRedis;
        private readonly string _DBAcoesEndpointUri;
        private readonly string _DBAcoesEndpointPrimaryKey;
        private readonly MongoClient _mongoClient;
        private readonly IMongoDatabase _mongoDatabase;
        private readonly IMongoCollection<CotacaoMoedaMongoDocument> _mongoCollection;

        public CotacoesRepository(IConfiguration configuration)
        {
            _conexaoRedis = ConnectionMultiplexer
                .Connect(configuration["RedisConnectionString"]);

            _prefixoCotacaoRedis = configuration["PrefixoCotacaoRedis"];

            _DBAcoesEndpointUri = configuration["DBAcoesEndpointUri"];
            _DBAcoesEndpointPrimaryKey = configuration["DBAcoesEndpointPrimaryKey"];

            _mongoClient = new MongoClient(configuration["MongoConnection"]);
            _mongoDatabase = _mongoClient.GetDatabase(
                configuration["MongoDatabase"]);
            _mongoCollection = _mongoDatabase
                .GetCollection<CotacaoMoedaMongoDocument>(
                    configuration["MongoCollection"]);
        }

        public CotacaoMoedaDocument GetCotacao(string id)
        {
            using (var client = new DocumentClient(
                new Uri(_DBAcoesEndpointUri), _DBAcoesEndpointPrimaryKey))
            {
                FeedOptions queryOptions =
                    new FeedOptions { MaxItemCount = -1 };

                return client.CreateDocumentQuery<CotacaoMoedaDocument>(
                        UriFactory.CreateDocumentCollectionUri(
                            "DBCotacoes", "HistoricoMoedas"),
                            "SELECT * FROM HistoricoMoedas h " +
                           $"WHERE h.id = '{id}'", queryOptions)
                        .ToArray()[0];
            }
        }

        public void SaveCotacaoAtual(CotacaoMoedaDocument document)
        {
            var dbRedis = _conexaoRedis.GetDatabase();
            dbRedis.StringSet(
                $"{_prefixoCotacaoRedis}-{document.Sigla}",
                JsonSerializer.Serialize(document, new JsonSerializerOptions()
                {
                    IgnoreNullValues = true
                }),
                expiry: null);
        }

        public void SaveHistoricoDolar(CotacaoMoedaDocument document)
        {
            _mongoCollection.InsertOne(new CotacaoMoedaMongoDocument()
            {
                CodHistorico = document.id,
                Sigla = document.Sigla,
                DataReferencia = document.DataReferencia,
                Valor = document.Valor
            });
        }
    }
}