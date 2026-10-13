db = db.getSiblingDB('shop');
db.customers.insertMany([
  { name: 'Ann', email: 'ann@example.com', tags: ['vip'], address: { city: 'Athens', street: 'Ermou 1' }, joined: new Date('2024-01-02') },
  { name: 'Ben', email: 'ben@example.com', tags: [], address: { city: 'Patras' }, joined: new Date('2024-03-04') },
]);
db.customers.createIndex({ email: 1 }, { unique: true });
