const express = require('express');
const cors = require('cors');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');

const app = express();
app.use(cors());
app.use(express.json());

// Initialize Database
let db;
(async () => {
    db = await open({
        filename: './database.sqlite',
        driver: sqlite3.Database
    });

    await db.exec(`
        CREATE TABLE IF NOT EXISTS Contact (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phoneNumber TEXT,
            email TEXT,
            linkedId INTEGER,
            linkPrecedence TEXT CHECK(linkPrecedence IN ('primary', 'secondary')),
            createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
            updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
            deletedAt DATETIME
        )
    `);
    console.log("Database connected and table ready.");
})();

app.post('/identify', async (req, res) => {
    const { email, phoneNumber } = req.body;

    if (!email && !phoneNumber) {
        return res.status(400).json({ error: "Email or phoneNumber is required" });
    }

    try {
        const matchingContacts = await db.all(
            `SELECT * FROM Contact WHERE email = ? OR phoneNumber = ?`,
            [email, phoneNumber]
        );

        if (matchingContacts.length === 0) {
            const result = await db.run(
                `INSERT INTO Contact (email, phoneNumber, linkPrecedence, createdAt, updatedAt) VALUES (?, ?, 'primary', ?, ?)`,
                [email, phoneNumber, new Date().toISOString(), new Date().toISOString()]
            );
            
            // Return early for new customer
            return res.status(200).json({
                contact: {
                    primaryContatctId: result.lastID,
                    emails: [email].filter(Boolean),
                    phoneNumbers: [phoneNumber].filter(Boolean),
                    secondaryContactIds: []
                }
            });
        }

        let primaryIds = new Set();
        matchingContacts.forEach(c => {
            if (c.linkPrecedence === 'primary') {
                primaryIds.add(c.id);
            } else {
                primaryIds.add(c.linkedId);
            }
        });

       
        const primaries = await db.all(
            `SELECT * FROM Contact WHERE id IN (${Array.from(primaryIds).join(',')}) ORDER BY createdAt ASC`
        );

        const primaryContact = primaries[0]; 

      
        if (primaries.length > 1) {
            const secondaryPrimaries = primaries.slice(1);
            
            for (let sec of secondaryPrimaries) {
                await db.run(
                    `UPDATE Contact SET linkPrecedence = 'secondary', linkedId = ?, updatedAt = ? WHERE id = ?`,
                    [primaryContact.id, new Date().toISOString(), sec.id]
                );
                await db.run(
                    `UPDATE Contact SET linkedId = ?, updatedAt = ? WHERE linkedId = ?`,
                    [primaryContact.id, new Date().toISOString(), sec.id]
                );
            }
        }

       
        const allLinkedContacts = await db.all(
            `SELECT * FROM Contact WHERE id = ? OR linkedId = ?`,
            [primaryContact.id, primaryContact.id]
        );

        const existingEmails = new Set(allLinkedContacts.map(c => c.email).filter(Boolean));
        const existingPhones = new Set(allLinkedContacts.map(c => c.phoneNumber).filter(Boolean));

        let hasNewData = false;
        if (email && !existingEmails.has(email)) hasNewData = true;
        if (phoneNumber && !existingPhones.has(phoneNumber)) hasNewData = true;

        if (hasNewData) {
            await db.run(
                `INSERT INTO Contact (email, phoneNumber, linkedId, linkPrecedence, createdAt, updatedAt) VALUES (?, ?, ?, 'secondary', ?, ?)`,
                [email, phoneNumber, primaryContact.id, new Date().toISOString(), new Date().toISOString()]
            );
            // Add new data to sets for the response
            if (email) existingEmails.add(email);
            if (phoneNumber) existingPhones.add(phoneNumber);
        }

        const finalSecondaries = await db.all(
            `SELECT id FROM Contact WHERE linkedId = ?`,
            [primaryContact.id]
        );

        res.status(200).json({
            contact: {
                primaryContatctId: primaryContact.id,
                emails: Array.from(existingEmails),       // Unique array
                phoneNumbers: Array.from(existingPhones), // Unique array
                secondaryContactIds: finalSecondaries.map(c => c.id)
            }
        });

    } catch (error) {
        console.error("Error processing request:", error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});