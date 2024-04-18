export default async function handler(req, res) {
  const { contractAddress, limit } = req.query;
  let data = {
    status: "success",
    data: {
      requestedAddress: contractAddress,
      wallets: [
        {
          walletAddress: "1XxX1xXXXxxXX1x1X1XXxX1X1X1xXxXxX",
          balance: "15000.00",
          transactionsCount: 120,
          rank: 1,
        },
        {
          walletAddress: "2YyY2yYYYyyYY2y2Y2YYyY2Y2Y2yYyYyY",
          balance: "14500.00",
          transactionsCount: 115,
          rank: 2,
        },
        {
          walletAddress: "3ZzZ3zZZZzzZZ3z3Z3ZZzZ3Z3Z3zZzZzZ",
          balance: "14000.00",
          transactionsCount: 110,
          rank: 3,
        },
      ],
    },
  };
  const realData = await fetch(
    `http://localhost:8080/base_chain/highest_wallets?address=${contractAddress}&limit=${
      limit ?? 3
    }`
  ).catch((err) => {
    console.log("fetch failed,", err);
  });
  try {
    if (realData) {
      data = realData;
    }
    res.status(200).json(data);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "An error occured." });
  }
}
