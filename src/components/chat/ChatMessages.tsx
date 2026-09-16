import { cn } from "../lib/utils";
import type { Ref } from "react";
import { useStickToBottom } from "../../hooks/useStickToBottom";
import { ChatMessage } from "./ChatMessage";
import type { Message } from "./types";

interface ChatMessagesProps {
  messages: Message[];
  emptyState?: React.ReactNode;
  onOpenNote?: (noteId: number) => void;
  className?: string;
  contentClassName?: string;
  contentRef?: Ref<HTMLDivElement>;
  sizeSource?: boolean;
}

export function ChatMessages({
  messages,
  emptyState,
  onOpenNote,
  className,
  contentClassName,
  contentRef,
  sizeSource = false,
}: ChatMessagesProps) {
  // Follow the stream only while the user is at the bottom; scrolling up to
  // re-read must not be yanked back down by the next token.
  const { scrollRef, handleScroll } = useStickToBottom<HTMLDivElement>(messages);

  return (
    <div
      ref={scrollRef}
      onScroll={handleScroll}
      className={cn("flex-1 overflow-y-auto agent-chat-scroll", "px-3 py-2", className)}
    >
      {messages.length === 0 ? (
        (emptyState ?? null)
      ) : (
        <div
          ref={contentRef}
          className={cn("flex flex-col gap-1.5", contentClassName)}
          data-panel-size-source={sizeSource ? "" : undefined}
        >
          {messages
            .filter((msg) => msg.role !== "tool")
            .map((msg) => (
              <ChatMessage
                key={msg.id}
                id={msg.id}
                role={msg.role as "user" | "assistant"}
                content={msg.content}
                isStreaming={msg.isStreaming}
                toolCalls={msg.toolCalls}
                onOpenNote={onOpenNote}
              />
            ))}
        </div>
      )}
    </div>
  );
}
