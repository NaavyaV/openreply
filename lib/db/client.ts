import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "@/app/generated/prisma/client";

const globalForPrisma = globalThis as unknown as {
  prisma?: PrismaClient;
};

function createPrismaClient() {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL environment variable is required");
  }

  // Few connections, short idle: Neon Free suspends ~5 minutes after the last
  // client disconnects (that delay cannot be shortened on Free). Drop idle
  // sockets quickly so the 5-minute clock can start as soon as a sweep or
  // webhook finishes.
  return new PrismaClient({
    adapter: new PrismaPg({
      connectionString: databaseUrl,
      max: 2,
      idleTimeoutMillis: 1_000,
      connectionTimeoutMillis: 10_000,
      allowExitOnIdle: true,
    }),
  });
}

export function getPrisma(): PrismaClient {
  if (!globalForPrisma.prisma) {
    globalForPrisma.prisma = createPrismaClient();
  }

  return globalForPrisma.prisma;
}

export const prisma = new Proxy({} as PrismaClient, {
  get(_target, prop, receiver) {
    return Reflect.get(getPrisma(), prop, receiver);
  },
});
