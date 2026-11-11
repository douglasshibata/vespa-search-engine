import React from 'react';

export const metadata = {
  title: 'Vespa Closed-Loop Real-Time Search',
  description: 'Next.js + Vespa.ai + Kafka + Go SSE Integration',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
