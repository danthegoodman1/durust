use proc_macro::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{
    parse_macro_input, Block, Expr, ExprAwait, ExprCall, ExprLit, FnArg, GenericArgument, ItemFn,
    Lit, LitStr, Meta, Pat, Path, PathArguments, ReturnType, Token, Type,
};

struct MacroArgs {
    items: Punctuated<Meta, Token![,]>,
}

impl Parse for MacroArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self {
            items: Punctuated::parse_terminated(input)?,
        })
    }
}

struct QueryArgs {
    workflow: Path,
}

impl Parse for QueryArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: syn::Ident = input.parse()?;
        if key != "workflow" {
            return Err(syn::Error::new_spanned(
                key,
                "#[durust::query] expects `workflow = workflow_name`",
            ));
        }
        input.parse::<Token![=]>()?;
        let workflow = input.parse::<Path>()?;
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
            if !input.is_empty() {
                return Err(syn::Error::new(
                    input.span(),
                    "unsupported durust query attribute argument",
                ));
            }
        }
        Ok(Self { workflow })
    }
}

struct JoinInput {
    futures: Punctuated<Expr, Token![,]>,
}

impl Parse for JoinInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self {
            futures: Punctuated::parse_terminated(input)?,
        })
    }
}

struct SelectInput {
    branches: Vec<SelectBranch>,
}

struct SelectBranch {
    pattern: Pat,
    future: Expr,
    body: Block,
}

impl Parse for SelectInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut branches = Vec::new();
        while !input.is_empty() {
            let pattern = Pat::parse_single(input)?;
            input.parse::<Token![=]>()?;
            let future = input.parse::<Expr>()?;
            input.parse::<Token![=>]>()?;
            let body = input.parse::<Block>()?;
            let _ = input.parse::<Option<Token![,]>>()?;
            branches.push(SelectBranch {
                pattern,
                future,
                body,
            });
        }
        Ok(Self { branches })
    }
}

#[proc_macro_attribute]
pub fn workflow(args: TokenStream, item: TokenStream) -> TokenStream {
    expand_handler(args, item, HandlerKind::Workflow)
}

#[proc_macro_attribute]
pub fn activity(args: TokenStream, item: TokenStream) -> TokenStream {
    expand_handler(args, item, HandlerKind::Activity)
}

#[proc_macro]
pub fn call_activity(input: TokenStream) -> TokenStream {
    let call = parse_macro_input!(input as ExprCall);
    match expand_call_activity(call) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn child(input: TokenStream) -> TokenStream {
    let call = parse_macro_input!(input as ExprCall);
    match expand_child(call) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn join(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as JoinInput);
    match expand_join(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn select(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as SelectInput);
    match expand_select(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn query(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as QueryArgs);
    let item_fn = parse_macro_input!(item as ItemFn);
    match expand_query(args, item_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

enum HandlerKind {
    Workflow,
    Activity,
}

fn expand_handler(args: TokenStream, item: TokenStream, kind: HandlerKind) -> TokenStream {
    let args = parse_macro_input!(args as MacroArgs);
    let item_fn = parse_macro_input!(item as ItemFn);

    match expand_handler_inner(args, item_fn, kind) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_handler_inner(
    args: MacroArgs,
    item_fn: ItemFn,
    kind: HandlerKind,
) -> syn::Result<proc_macro2::TokenStream> {
    if item_fn.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.fn_token,
            "durust handlers must be async functions",
        ));
    }

    let parsed = ParsedArgs::from(args)?;
    if matches!(kind, HandlerKind::Activity) && parsed.query_state.is_some() {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.ident,
            "#[durust::activity] does not support `query_state`",
        ));
    }
    if matches!(kind, HandlerKind::Workflow) {
        lint_workflow_body(&item_fn)?;
    }

    let vis = &item_fn.vis;
    let attrs = &item_fn.attrs;
    let ident = &item_fn.sig.ident;
    let impl_ident = format_ident!("__durust_impl_{}", ident);
    let manifest_ident = format_ident!("__durust_manifest_{}", ident);
    let (input_binding, input) = extract_single_input(&item_fn)?;
    let output = extract_result_output(&item_fn)?;
    let query_state = parsed
        .query_state
        .clone()
        .unwrap_or_else(|| syn::parse_quote!(()));
    let rust_path =
        quote!(concat!(env!("CARGO_PKG_NAME"), "::", module_path!(), "::", stringify!(#ident)));
    let name = match parsed.name {
        Some(name) => quote!(#name),
        None => rust_path.clone(),
    };
    let block = &item_fn.block;

    let version_impl = match kind {
        HandlerKind::Workflow => {
            let version = parsed.version.ok_or_else(|| {
                syn::Error::new_spanned(
                    &item_fn.sig.ident,
                    "#[durust::workflow] requires `version = <u32>`",
                )
            })?;
            quote! {
                const VERSION: u32 = #version;
            }
        }
        HandlerKind::Activity => quote! {},
    };

    let trait_name = match kind {
        HandlerKind::Workflow => quote!(::durust::Workflow),
        HandlerKind::Activity => quote!(::durust::Activity),
    };

    let run_fn = match kind {
        HandlerKind::Workflow => quote! {
            fn run(self, input: Self::Input) -> ::durust::BoxWorkflowFuture<Self::Output> {
                Box::pin(async move { #impl_ident(input).await })
            }
        },
        HandlerKind::Activity => quote! {
            fn run(self, input: Self::Input) -> ::durust::BoxActivityFuture<Self::Output> {
                Box::pin(async move { #impl_ident(input).await })
            }
        },
    };

    let manifest_export = match kind {
        HandlerKind::Workflow => quote! {
            #[doc(hidden)]
            #[allow(non_snake_case)]
            fn #manifest_ident() -> ::durust::ManifestWorkflow {
                ::durust::ManifestWorkflow {
                    name: <#ident as ::durust::Workflow>::NAME.to_owned(),
                    version: <#ident as ::durust::Workflow>::VERSION,
                    rust_path: <#ident as ::durust::Workflow>::RUST_PATH.to_owned(),
                    input_type: <#ident as ::durust::Workflow>::input_type_name().to_owned(),
                    output_type: <#ident as ::durust::Workflow>::output_type_name().to_owned(),
                    query_state_type: <#ident as ::durust::Workflow>::query_state_type_name()
                        .map(str::to_owned),
                    input_schema_hash: ::durust::type_name_fingerprint(
                        <#ident as ::durust::Workflow>::input_type_name(),
                    ),
                    output_schema_hash: ::durust::type_name_fingerprint(
                        <#ident as ::durust::Workflow>::output_type_name(),
                    ),
                    query_state_schema_hash: <#ident as ::durust::Workflow>::query_state_type_name()
                        .map(::durust::type_name_fingerprint),
                }
            }

            ::durust::inventory::submit! {
                ::durust::DurableExport::Workflow(#manifest_ident)
            }
        },
        HandlerKind::Activity => quote! {
            #[doc(hidden)]
            #[allow(non_snake_case)]
            fn #manifest_ident() -> ::durust::ManifestActivity {
                ::durust::ManifestActivity {
                    name: <#ident as ::durust::Activity>::NAME.to_owned(),
                    rust_path: <#ident as ::durust::Activity>::RUST_PATH.to_owned(),
                    input_type: <#ident as ::durust::Activity>::input_type_name().to_owned(),
                    output_type: <#ident as ::durust::Activity>::output_type_name().to_owned(),
                    input_schema_hash: ::durust::type_name_fingerprint(
                        <#ident as ::durust::Activity>::input_type_name(),
                    ),
                    output_schema_hash: ::durust::type_name_fingerprint(
                        <#ident as ::durust::Activity>::output_type_name(),
                    ),
                }
            }

            ::durust::inventory::submit! {
                ::durust::DurableExport::Activity(#manifest_ident)
            }
        },
    };
    let query_state_impl = match kind {
        HandlerKind::Workflow => quote! {
            type QueryState = #query_state;
        },
        HandlerKind::Activity => quote! {},
    };

    Ok(quote! {
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug, Default)]
        #vis struct #ident;

        impl #trait_name for #ident {
            type Input = #input;
            type Output = #output;
            #query_state_impl

            const NAME: &'static str = #name;
            #version_impl
            const RUST_PATH: &'static str = #rust_path;

            #run_fn
        }

        #manifest_export

        #(#attrs)*
        async fn #impl_ident(input: #input) -> ::durust::Result<#output> {
            let #input_binding = input;
            #block
        }
    })
}

fn expand_call_activity(call: ExprCall) -> syn::Result<proc_macro2::TokenStream> {
    if call.args.len() != 1 {
        return Err(syn::Error::new_spanned(
            call,
            "durust::call_activity! expects exactly one activity input",
        ));
    }
    let activity = call.func;
    let input = call.args.first().expect("checked arg count");
    Ok(quote! {
        ::durust::activity_call::<#activity>(#input)
    })
}

fn expand_child(call: ExprCall) -> syn::Result<proc_macro2::TokenStream> {
    if call.args.len() != 1 {
        return Err(syn::Error::new_spanned(
            call,
            "durust::child! expects exactly one workflow input",
        ));
    }
    let workflow = call.func;
    let input = call.args.first().expect("checked arg count");
    Ok(quote! {
        ::durust::child_workflow::<#workflow>(#input)
    })
}

fn expand_query(args: QueryArgs, item_fn: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    if item_fn.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.asyncness,
            "durust query handlers must be synchronous functions",
        ));
    }
    if item_fn.sig.inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.inputs,
            "durust query handlers must take exactly one query-state reference",
        ));
    }
    let FnArg::Typed(arg) = item_fn.sig.inputs.first().expect("checked input count") else {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.inputs,
            "durust query handlers cannot take self",
        ));
    };
    let Type::Reference(reference) = arg.ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            &arg.ty,
            "durust query handlers must take `&<workflow query state>`",
        ));
    };
    let query_state = &reference.elem;
    let workflow = args.workflow;
    let attrs = &item_fn.attrs;
    let vis = &item_fn.vis;
    let sig = &item_fn.sig;
    let block = &item_fn.block;

    Ok(quote! {
        const _: fn() = || {
            fn __durust_query_state_check()
            where
                #workflow: ::durust::Workflow<QueryState = #query_state>,
            {
            }
            let _ = __durust_query_state_check;
        };

        #(#attrs)*
        #vis #sig #block
    })
}

fn expand_join(input: JoinInput) -> syn::Result<proc_macro2::TokenStream> {
    let futures = input.futures.into_iter().collect::<Vec<_>>();
    if futures.len() < 2 {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "durust::join! expects at least two durable futures",
        ));
    }

    let future_vars = (0..futures.len())
        .map(|index| format_ident!("__durust_join_future_{index}"))
        .collect::<Vec<_>>();
    let output_vars = (0..futures.len())
        .map(|index| format_ident!("__durust_join_output_{index}"))
        .collect::<Vec<_>>();
    let output_messages = (0..futures.len())
        .map(|index| {
            LitStr::new(
                &format!("join branch {index} completed without output"),
                proc_macro2::Span::call_site(),
            )
        })
        .collect::<Vec<_>>();

    let poll_branches =
        future_vars
            .iter()
            .zip(output_vars.iter())
            .map(|(future_var, output_var)| {
                quote! {
                    if #output_var.is_none() {
                        match ::std::future::Future::poll(
                            ::std::pin::Pin::new(&mut #future_var),
                            __durust_cx,
                        ) {
                            ::std::task::Poll::Ready(::std::result::Result::Ok(__durust_join_value)) => {
                                #output_var = ::std::option::Option::Some(__durust_join_value);
                                __durust_join_made_progress = true;
                            }
                            ::std::task::Poll::Ready(::std::result::Result::Err(__durust_join_err)) => {
                                if __durust_join_first_error.is_none() {
                                    __durust_join_first_error = ::std::option::Option::Some(__durust_join_err);
                                }
                                __durust_join_made_progress = true;
                            }
                            ::std::task::Poll::Pending => {}
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
    let output_ready = output_vars.iter().map(|output_var| {
        quote! {
            #output_var.is_some()
        }
    });
    let take_outputs =
        output_vars
            .iter()
            .zip(output_messages.iter())
            .map(|(output_var, output_message)| {
                quote! {
                    #output_var.take().expect(#output_message)
                }
            });

    Ok(quote! {{
        #(let mut #future_vars = #futures;)*
        #(::durust::__durust_join_assert_branch(&#future_vars);)*
        #(let mut #output_vars = ::std::option::Option::None;)*
        ::std::future::poll_fn(move |__durust_cx| {
            loop {
                let mut __durust_join_made_progress = false;
                let mut __durust_join_first_error = ::std::option::Option::None;
                #(#poll_branches)*
                if let ::std::option::Option::Some(__durust_join_err) = __durust_join_first_error {
                    return ::std::task::Poll::Ready(::std::result::Result::Err(__durust_join_err));
                }
                if true #(&& #output_ready)* {
                    return ::std::task::Poll::Ready(::std::result::Result::Ok((#(#take_outputs),*)));
                }
                if !__durust_join_made_progress {
                    return ::std::task::Poll::Pending;
                }
            }
        })
    }})
}

fn expand_select(input: SelectInput) -> syn::Result<proc_macro2::TokenStream> {
    let branches = input.branches;
    if branches.len() < 2 {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "durust::select! expects at least two durable futures",
        ));
    }

    let branch_digest = branches
        .iter()
        .map(|branch| {
            format!(
                "{}={}",
                branch.pattern.to_token_stream(),
                branch.future.to_token_stream()
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let branch_digest = LitStr::new(&branch_digest, proc_macro2::Span::call_site());

    let futures = branches
        .iter()
        .map(|branch| &branch.future)
        .collect::<Vec<_>>();
    let patterns = branches
        .iter()
        .map(|branch| &branch.pattern)
        .collect::<Vec<_>>();
    let bodies = branches.iter().map(|branch| &branch.body).collect::<Vec<_>>();
    let future_vars = (0..branches.len())
        .map(|index| format_ident!("__durust_select_future_{index}"))
        .collect::<Vec<_>>();
    let output_vars = (0..branches.len())
        .map(|index| format_ident!("__durust_select_output_{index}"))
        .collect::<Vec<_>>();
    let branch_ordinals = (0..branches.len())
        .map(|index| {
            syn::LitInt::new(
                &format!("{index}_u32"),
                proc_macro2::Span::call_site(),
            )
        })
        .collect::<Vec<_>>();
    let output_messages = (0..branches.len())
        .map(|index| {
            LitStr::new(
                &format!("select branch {index} selected without output"),
                proc_macro2::Span::call_site(),
            )
        })
        .collect::<Vec<_>>();

    let poll_branches =
        future_vars
            .iter()
            .zip(output_vars.iter())
            .map(|(future_var, output_var)| {
                quote! {
                    if #output_var.is_none() {
                        ::durust::__durust_select_clear_ready_event_id();
                        match ::std::future::Future::poll(
                            ::std::pin::Pin::new(&mut #future_var),
                            __durust_cx,
                        ) {
                            ::std::task::Poll::Ready(::std::result::Result::Ok(__durust_select_value)) => {
                                let __durust_select_event_id =
                                    ::durust::__durust_select_take_ready_event_id()
                                        .unwrap_or(::durust::EventId::ZERO);
                                #output_var = ::std::option::Option::Some((
                                    __durust_select_event_id,
                                    ::std::result::Result::Ok(__durust_select_value),
                                ));
                            }
                            ::std::task::Poll::Ready(::std::result::Result::Err(__durust_select_err)) => {
                                if let ::std::option::Option::Some(__durust_select_event_id) =
                                    ::durust::__durust_select_take_ready_event_id()
                                {
                                    #output_var = ::std::option::Option::Some((
                                        __durust_select_event_id,
                                        ::std::result::Result::Err(__durust_select_err),
                                    ));
                                } else {
                                    return ::std::task::Poll::Ready(
                                        ::std::result::Result::Err(__durust_select_err),
                                    );
                                }
                            }
                            ::std::task::Poll::Pending => {}
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
    let select_ready =
        output_vars
            .iter()
            .zip(branch_ordinals.iter())
            .map(|(output_var, branch_ordinal)| {
                quote! {
                    if let ::std::option::Option::Some((__durust_select_event_id, _)) =
                        #output_var.as_ref()
                    {
                        match __durust_select_winner {
                            ::std::option::Option::Some((
                                __durust_select_winner_ordinal,
                                __durust_select_winner_event_id,
                            )) if (__durust_select_winner_event_id, __durust_select_winner_ordinal)
                                <= (*__durust_select_event_id, #branch_ordinal) => {}
                            _ => {
                                __durust_select_winner =
                                    ::std::option::Option::Some((#branch_ordinal, *__durust_select_event_id));
                            }
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
    let cancel_losers =
        future_vars
            .iter()
            .zip(output_vars.iter())
            .zip(branch_ordinals.iter())
            .map(|((future_var, output_var), branch_ordinal)| {
                quote! {
                    if __durust_select_branch_ordinal != #branch_ordinal && #output_var.is_none() {
                        ::durust::DurableSelectBranch::__durust_cancel_branch(&#future_var);
                    }
                }
            })
            .collect::<Vec<_>>();
    let match_arms = branch_ordinals
        .iter()
        .zip(patterns.iter())
        .zip(output_vars.iter())
        .zip(output_messages.iter())
        .zip(bodies.iter())
        .map(
            |((((branch_ordinal, pattern), output_var), output_message), body)| {
                quote! {
                    #branch_ordinal => {
                        let (_, #pattern) = #output_var.take().expect(#output_message);
                        #body
                    }
                }
            },
        )
        .collect::<Vec<_>>();

    Ok(quote! {{
        #(let mut #future_vars = #futures;)*
        #(let mut #output_vars = ::std::option::Option::None;)*
        let mut __durust_select_command_id = ::std::option::Option::None;
        let __durust_select_branch_ordinal = ::std::future::poll_fn(|__durust_cx| {
            ::durust::__durust_select_ensure_command_id(&mut __durust_select_command_id);
            #(#poll_branches)*

            let mut __durust_select_winner:
                ::std::option::Option<(u32, ::durust::EventId)> = ::std::option::Option::None;
            #(#select_ready)*
            let ::std::option::Option::Some((
                __durust_select_branch_ordinal,
                __durust_select_winning_event_id,
            )) = __durust_select_winner else {
                return ::std::task::Poll::Pending;
            };
            let __durust_select_command_id = __durust_select_command_id
                .as_ref()
                .expect("select command id initialized");
            match ::durust::__durust_select_record_winner(
                __durust_select_command_id,
                __durust_select_branch_ordinal,
                __durust_select_winning_event_id,
                #branch_digest,
            ) {
                ::std::task::Poll::Ready(::std::result::Result::Ok(())) => {
                    #(#cancel_losers)*
                    ::std::task::Poll::Ready(::std::result::Result::Ok(
                        __durust_select_branch_ordinal,
                    ))
                }
                ::std::task::Poll::Ready(::std::result::Result::Err(__durust_select_err)) => {
                    ::std::task::Poll::Ready(::std::result::Result::Err(__durust_select_err))
                }
                ::std::task::Poll::Pending => ::std::task::Poll::Pending,
            }
        })
        .await?;

        match __durust_select_branch_ordinal {
            #(#match_arms)*
            _ => ::std::unreachable!("select returned a branch ordinal outside macro range"),
        }
    }})
}

fn extract_single_input(item_fn: &ItemFn) -> syn::Result<(Pat, Type)> {
    if item_fn.sig.inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.inputs,
            "durust handlers must take exactly one input argument",
        ));
    }

    match item_fn.sig.inputs.first().expect("checked input count") {
        FnArg::Typed(arg) => Ok(((*arg.pat).clone(), (*arg.ty).clone())),
        FnArg::Receiver(receiver) => Err(syn::Error::new_spanned(
            receiver,
            "durust handlers cannot take self",
        )),
    }
}

fn extract_result_output(item_fn: &ItemFn) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = &item_fn.sig.output else {
        return Err(syn::Error::new_spanned(
            &item_fn.sig,
            "durust handlers must return durust::Result<T>",
        ));
    };

    let Type::Path(type_path) = ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    };

    let Some(segment) = type_path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    };

    if segment.ident != "Result" {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    };

    let Some(GenericArgument::Type(output)) = args.args.first() else {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    };

    Ok(output.clone())
}

#[derive(Default)]
struct ParsedArgs {
    name: Option<String>,
    version: Option<u32>,
    query_state: Option<Type>,
}

impl ParsedArgs {
    fn from(args: MacroArgs) -> syn::Result<Self> {
        let mut parsed = Self::default();
        for meta in args.items {
            match meta {
                Meta::Path(path) if path.is_ident("strict") => {}
                Meta::NameValue(name_value) if name_value.path.is_ident("name") => {
                    parsed.name = Some(lit_string(&name_value.value, "name")?);
                }
                Meta::NameValue(name_value) if name_value.path.is_ident("version") => {
                    parsed.version = Some(lit_u32(&name_value.value, "version")?);
                }
                Meta::NameValue(name_value) if name_value.path.is_ident("query_state") => {
                    parsed.query_state = Some(syn::parse2(name_value.value.to_token_stream())?);
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unsupported durust handler attribute argument",
                    ));
                }
            }
        }
        Ok(parsed)
    }
}

fn lit_string(expr: &Expr, field: &str) -> syn::Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.value()),
        _ => Err(syn::Error::new_spanned(
            expr,
            format!("`{field}` must be a string literal"),
        )),
    }
}

fn lit_u32(expr: &Expr, field: &str) -> syn::Result<u32> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(value),
            ..
        }) => value.base10_parse(),
        _ => Err(syn::Error::new_spanned(
            expr,
            format!("`{field}` must be an integer literal"),
        )),
    }
}

fn lint_workflow_body(item_fn: &ItemFn) -> syn::Result<()> {
    let source = item_fn.block.to_token_stream().to_string();
    let forbidden = [
        ("tokio :: time :: sleep", "use durust::sleep instead"),
        ("tokio :: select", "use durust::select! instead"),
        ("tokio :: spawn", "use durust::spawn or durust::join! instead"),
        ("std :: time :: Instant :: now", "use durust::now instead"),
        ("std :: time :: SystemTime :: now", "use durust::now instead"),
        ("rand :: random", "use durust::side_effect instead"),
    ];

    for (needle, suggestion) in forbidden {
        if source.contains(needle) {
            return Err(syn::Error::new_spanned(
                &item_fn.sig.ident,
                format!("nondeterministic workflow API detected; {suggestion}"),
            ));
        }
    }

    let mut await_lint = AwaitLint::default();
    await_lint.visit_block(&item_fn.block);
    if let Some(err) = await_lint.err {
        return Err(err);
    }

    Ok(())
}

#[derive(Default)]
struct AwaitLint {
    err: Option<syn::Error>,
}

impl<'ast> Visit<'ast> for AwaitLint {
    fn visit_expr_await(&mut self, node: &'ast ExprAwait) {
        if self.err.is_some() {
            return;
        }

        let base = node.base.to_token_stream().to_string();
        let result_method = matches!(
            node.base.as_ref(),
            Expr::MethodCall(method) if method.method == "result"
        );
        let allowed = base.contains("durust :: activity_call")
            || base.contains("durust :: call_activity")
            || base.contains("durust :: child")
            || base.contains("durust :: child_workflow")
            || base.contains("durust :: activity_map")
            || base.contains("result_manifest")
            || result_method
            || base.contains("durust :: sleep")
            || base.contains("durust :: sleep_until")
            || base.contains("durust :: signal")
            || base.contains("durust :: select_all")
            || base.contains("durust :: join");
        if !allowed {
            self.err = Some(syn::Error::new_spanned(
                node,
                "unknown await in workflow code; use durable APIs such as durust::call_activity!",
            ));
            return;
        }

        syn::visit::visit_expr_await(self, node);
    }
}
