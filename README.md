# Cricket Centre

Ball-by-ball scoring for a friendly tournament, with a shareable live scoreboard.
Developed by Anthony Dennis.

- A scorer link enters every ball. A separate view-only link can be shared with anyone.
- 1 to 10 overs per match. Wide and no-ball penalties and a bowler over limit are set per match.
- Data is an append-only ball log. Undo removes the last entry. Scorecards are replayed from the log.
- Storage: Postgres when `DATABASE_URL` is set, otherwise a temporary file.

Run: `npm install && npm start`. Test: `npm test`.
