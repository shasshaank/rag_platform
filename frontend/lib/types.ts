export type Citation = {
    idx: number;
    score?:number|null;
    doc_id?:string|null;
    filename?:string|null;
    page?:number|null;
    chunk_id?:number|null;
    text_preview?:string|null;
};

export type UserMessage = {role:'user';content:string};

export type AssistantMessage = {role:'assistant';
    content:string;
    citations?:Citation[]
};

export type Message = UserMessage | AssistantMessage;