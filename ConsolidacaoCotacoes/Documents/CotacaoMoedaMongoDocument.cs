using MongoDB.Bson;

namespace ConsolidacaoCotacoes.Documents
{
    public class CotacaoMoedaMongoDocument
    {
        public ObjectId _id { get; set; }
        public string CodHistorico { get; set; }
        public string Sigla { get; set; }
        public string DataReferencia { get; set; }
        public double Valor { get; set; }
    }
}