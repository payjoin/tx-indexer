# Rust Wallet Fingerprinting

Wallet fingerprints are identifiable patterns in transaction structure that can reveal the use of specific wallet software. While these patterns alone may not yield actionable insights, they enhance the effectiveness of other privacy heuristics. By combining fingerprinting with techniques from [Resurrecting Address Clustering in Bitcoin](https://link.springer.com/chapter/10.1007/978-3-031-18283-9_19) and [How to Peel a Million](https://www.usenix.org/conference/usenixsecurity22/presentation/kappos), analysts can cluster related transactions and addresses more accurately.

This project is a Rust port of the original [Python implementation](https://github.com/ishaanam/wallet-fingerprinting/tree/master).
