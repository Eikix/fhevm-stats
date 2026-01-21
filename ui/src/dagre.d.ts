declare module "@dagrejs/dagre" {
  const dagre: {
    graphlib: {
      Graph: new (options?: {
        multigraph?: boolean;
        compound?: boolean;
      }) => {
        setGraph: (config: Record<string, number | string>) => void;
        setDefaultEdgeLabel: (fn: () => Record<string, unknown>) => void;
        setNode: (id: string, data: { width: number; height: number; label?: string }) => void;
        setEdge: (
          edge: { v: string; w: string; name?: string },
          data?: Record<string, unknown>,
        ) => void;
        nodes: () => string[];
        edges: () => Array<{ v: string; w: string; name?: string }>;
        node: (id: string) => { x: number; y: number; width: number; height: number };
        edge: (edge: { v: string; w: string; name?: string }) => {
          points?: Array<{ x: number; y: number }>;
        };
      };
    };
    layout: (graph: unknown) => void;
  };
  export default dagre;
}
