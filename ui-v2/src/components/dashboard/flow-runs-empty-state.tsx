import type { JSX } from "react";

import { DocsLink } from "@/components/ui/docs-link";
import {
	EmptyState,
	EmptyStateActions,
	EmptyStateDescription,
	EmptyStateIcon,
	EmptyStateTitle,
} from "@/components/ui/empty-state";

export const DashboardFlowRunsEmptyState = (): JSX.Element => (
	<EmptyState>
		<EmptyStateIcon id="Workflow" />
		<EmptyStateTitle>No runs yet — kick things off!</EmptyStateTitle>
		<EmptyStateDescription>
			Each time a task or flow executes, a run is recorded here with its full
			state history and logs.
		</EmptyStateDescription>
		<EmptyStateActions>
			<DocsLink id="getting-started" />
		</EmptyStateActions>
	</EmptyState>
);
