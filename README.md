# AIDB

Has nothing to do with AI but the user interface looks like you are talking to a chatbot.

Try it out at [nk0.me/g/aidb](https://nk0.me/g/aidb), or run it locally with `cargo run -p aidb-cli` and connect to it with any MySQL-compatible client.

## Features

- [ ] Schema storage
- [ ] Int, Real and Text datatypes
- [ ] CREATE TABLE statement
- [ ] Storage engine
- [ ] Logical query plan and physical query plan
- [ ] Query engine
- [ ] INSERT INTO statement
- [ ] SELECT statement
- [ ] UPDATE statement
- [ ] DELETE FROM statement
- [ ] B-Tree index
- [ ] Hash index
- [ ] Prefix index
- [ ] CREATE INDEX statement
- [ ] Transaction
- [ ] Write Ahead Journal
- [ ] START TRANSACTION and COMMIT statement
- [ ] Fancy browser-only Web-UI (via OPFS)
- [ ] Mostly MySQL-compatible server
- [x] Absolutely 0% AI (except for the name)

## Info for nerds

Install Trunk, run `trunk serve` to bring up the dev server for the Web-UI.

There are four crates in this workspace:

- aidb (root): UI, which is the most important part of the project and hence the name
- aidb-core: database implementation
- aidb-cli: MySQL adaptor
- archive: save the entire storage backend into or load from a tar.lz4 archive

Storage backend uses Apache OpenDAL.

#### Info for lawyers

<sup>
Copyright &copy; 2025 NKID00
<br>
This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
<br>
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
<br>
You should have received a copy of the GNU Affero General Public License along with this program. If not, see &lt;<a href="https://www.gnu.org/licenses/" target="_blank">https://www.gnu.org/licenses/</a>&gt;.
</sup>
