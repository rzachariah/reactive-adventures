<Query Kind="Program">
  <NuGetReference>Rx-Main</NuGetReference>
  <Namespace>System</Namespace>
  <Namespace>System.Reactive</Namespace>
  <Namespace>System.Reactive.Disposables</Namespace>
  <Namespace>System.Reactive.Linq</Namespace>
</Query>

void Main()
{
	var prices = Observable.Interval(TimeSpan.FromMilliseconds(100))
						   .Select(_ => Price.CreateNext())
						   .Publish()
						   .RefCount()
						   .Dump()
						   ;
						   
	var trades = Observable.Interval(TimeSpan.FromMilliseconds(700))
						   .Select(_ => Trade.CreateNext())
						   .Publish()
						   .RefCount()
						   .Dump()
						   ;
	
	var positions = trades.GroupBy(t => t.Symbol)
						        .Select(g => g.Scan(new Position(g.Key, 0), (current, next) => new Position(g.Key, current.Amount + next.Amount)))
								.Merge()
								.Publish()
								.RefCount()
								.Dump()
								;

	var valuations = positions.GroupBy(p => p.Symbol)
							  .Select(g => new {Key = g.Key, PositionStream = g, PriceStream = prices.Where(p => p.Symbol == g.Key)})
							  .Select(o => o.PositionStream.CombineLatest(o.PriceStream, (pos, price) => new CalculationSpec(pos, price, "MarketValue"))
							  							   .Sample(TimeSpan.FromSeconds(2))
														   .Select(spec => Calculator.Calculate(spec))
														   .Switch()
									 )
							  .Publish()
							  .RefCount()
							  .Dump();
}

public static class SecurityMaster
{
	private static string[] universe = new string[]{"IBM", "CSCO", "MSFT", "AAPL", "EZE"};
	
	public static IList<string> SymbolList
	{
		get { return universe.ToList(); }
	}
}

// Define other methods and classes here
public class Price
{
	private static Random rand = new Random();
	
	public string Symbol { get; private set; }
	public decimal Value { get; private set; }
	public DateTime TimeStamp { get; private set; }
	
	public Price(string symbol, decimal value)
	{
		Symbol = symbol;
		Value = value;
		TimeStamp = DateTime.Now;
	}
	
	public static Price CreateNext()
	{
		return new Price(SecurityMaster.SymbolList[rand.Next(0, SecurityMaster.SymbolList.Count())], rand.Next(100, 10000)/100m);
	}
}

public class Trade
{
	private static Random rand = new Random();

	public string Symbol { get; private set; }
	public decimal Amount { get; private set; }
	public DateTime TimeStamp { get; private set; }

	public Trade(string symbol, decimal amount)
	{
		Symbol = symbol;
		Amount = amount;
		TimeStamp = DateTime.Now;
	}

	public static Trade CreateNext()
	{
		return new Trade(SecurityMaster.SymbolList[rand.Next(0, SecurityMaster.SymbolList.Count())], rand.Next(1, 10)*100);
	}
}

public class Position
{
	public string Symbol { get; private set; }
	public decimal Amount { get; private set; }
	public DateTime TimeStamp { get; private set; }

	public Position(string symbol, decimal amount)
	{
		Symbol = symbol;
		Amount = amount;
		TimeStamp = DateTime.Now;
	}
}

public class CalculationSpec
{
	private string[] fields;
	
	public Position Position { get; private set; }
	public Price Price { get; private set; }
	public IEnumerable<string> Fields
	{
		get
		{
			return fields.AsEnumerable();
		}
	}
	
	public CalculationSpec(Position position, Price price, params string[] fields)
	{
		Position = position;
		Price = price;
		this.fields = fields;
	}
}

public static class Calculator
{
	private static Random rand = new Random();
	
	public static IObservable<object> Calculate(CalculationSpec spec)
	{
		var calculationLatency = rand.Next(0, 5);
		return Observable.Timer(TimeSpan.FromSeconds(calculationLatency))
						 .Select(_ =>
		{
			var pos = spec.Position;
			var price = spec.Price;
			var values = new Dictionary<string, object>();
			foreach(string field in spec.Fields)
			{
				decimal value;
				if (field == "MarketValue")
				{
					value = pos.Amount * price.Value;
					values[field] = value;
				}
			}
			values["TimeStamp"] = DateTime.Now.ToLongTimeString();
			var result = new {TimeStamp = values["TimeStamp"], Symbol = pos.Symbol, Amount = pos.Amount, Price = price.Value, MarketValue = values["MarketValue"], PositionTickedAt = pos.TimeStamp.ToLongTimeString(), PriceTickedAt = price.TimeStamp.ToLongTimeString(), CalcLatency = calculationLatency};
			return result;
		});
	}
}