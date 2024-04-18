export default async function handler(req, res) {
  const { testId } = req.query;
  try {
    res.status(200).json({ testId });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "An error occured." });
  }
}
