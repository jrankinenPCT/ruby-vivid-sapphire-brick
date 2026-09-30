import { createFileRoute } from "@tanstack/react-router";
import { OeeBench } from "@/components/oee-bench";

export const Route = createFileRoute("/")({ component: Home });

function Home() {
  return <OeeBench />;
}
