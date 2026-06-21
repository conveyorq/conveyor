import { Component, type ErrorInfo, type ReactNode } from "react";

// ErrorBoundary catches a render-time crash in its subtree and shows a
// recoverable fallback instead of letting the whole app unmount to a blank
// page. The shell wraps each view in one (keyed by the active tab), so a single
// bad view never takes down the navigation, theme, or token field.
interface Props {
  children: ReactNode;
}

interface State {
  error?: Error;
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = {};

  // getDerivedStateFromError records the error so the next render shows the
  // fallback.
  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  // componentDidCatch logs the crash for the operator's browser console.
  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("dashboard view crashed", error, info);
  }

  // reset clears the error so the subtree re-renders (e.g. after the user
  // retries or the underlying data changes).
  reset = (): void => {
    this.setState({ error: undefined });
  };

  render(): ReactNode {
    const { error } = this.state;
    if (error === undefined) {
      return this.props.children;
    }

    return (
      <div
        role="alert"
        className="rounded-lg border border-rose-500/30 bg-rose-50 px-5 py-4 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-300"
      >
        <p className="font-medium">This view hit an unexpected error.</p>
        <p className="mt-1 break-all text-rose-600/90 dark:text-rose-300/80">{error.message}</p>
        <button
          type="button"
          onClick={this.reset}
          className="mt-3 rounded-md bg-rose-600/10 px-3 py-1 text-xs font-medium text-rose-700 hover:bg-rose-600/20 dark:text-rose-200"
        >
          Try again
        </button>
      </div>
    );
  }
}
